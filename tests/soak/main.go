// Command soak runs a long mixed S3 workload against stupid-simple-s3 and
// checks that every object reads back exactly as written, and that the
// server leaks no goroutines, file descriptors, memory, temp files or
// multipart uploads.
//
// Workers own disjoint sets of keys and know what each must hold, so every
// read is checked byte for byte, and listings must match exactly. Keys
// shared by all workers are written concurrently, and every read of one
// must be a complete object written to it, with its own ETag. The workload
// includes range, conditional, copy, multipart and batch delete requests,
// uploads interrupted halfway, uploads whose body does not match the
// signed hash, and downloads abandoned halfway.
//
// At every checkpoint the workers stop, every object is read back, and
// once the server is idle its goroutines and open file descriptors must be
// back at the baseline taken after the first fill, with no temp files or
// multipart uploads left. At the end, the live heap after a GC must not
// have grown past -heap-growth times the baseline, nor grown steadily by
// more than -heap-slope bytes per operation over the later half of the
// checkpoints.
//
// The load is limited by -rate operations and -write-rate bytes written per
// second. The defaults are kept low on purpose: running unlimited against
// a fast server writes gigabytes per second, which made an NVMe drive drop
// off the bus in an 8 hour run. The leak and content checks need many
// operations, not many bytes.
//
// With -server, soak starts the server itself, on free ports and fresh
// directories under -workdir. Otherwise it tests the server at -endpoint,
// whose metrics listener must be at -metrics, with pprof enabled for the
// heap check.
//
// Run from the repository root:
//
//	make build && go run ./tests/soak -server bin/stupid-simple-s3 -duration 1h
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const region = "us-east-1"

type config struct {
	endpoint       string
	metricsURL     string
	accessKey      string
	secretKey      string
	bucket         string
	server         string
	workdir        string
	dataDir        string
	multipartDir   string
	duration       time.Duration
	interval       time.Duration
	workers        int
	keysPerWorker  int
	sharedKeys     int
	maxSize        int64
	rate           float64
	writeRate      int64
	seed           uint64
	settleTimeout  time.Duration
	goroutineSlack float64
	fdSlack        float64
	heapGrowth     float64
	heapSlope      float64
	multipartShare int
	keep           bool
}

type opStats struct {
	mu     sync.Mutex
	counts map[string]int64
	fails  map[string]int64
	// sent and received count object bytes. written estimates the bytes
	// the server writes for them: uploads, including refused and broken
	// off ones, copies, and the assembly of multipart uploads.
	sent, received, written atomic.Int64
}

func (st *opStats) count(op string) {
	st.mu.Lock()
	st.counts[op]++
	st.mu.Unlock()
}

func (st *opStats) total() int64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	var n int64
	for _, c := range st.counts {
		n += c
	}
	return n
}

type soak struct {
	cfg        config
	client     *s3.Client
	presign    *s3.PresignClient
	signer     *v4.Signer
	creds      aws.Credentials
	transport  *http.Transport
	httpClient *http.Client
	// rawClient sends the interrupted and tampered uploads, on connections
	// of their own: an upload broken off halfway once made a later request
	// on the same pooled connection fail with the upload's error
	rawClient     *http.Client
	monitorClient *http.Client
	gate          sync.RWMutex
	workers       []*worker
	stats         opStats
	opLimit       *limiter
	writeLimit    *limiter
	failures      atomic.Int64
	samples       []sample
	heapChecked   bool
	warnedGC      bool
	serverExited  chan error
}

// maxReported is the number of failures printed in full
const maxReported = 200

func (s *soak) fail(op, key, msg string) {
	n := s.failures.Add(1)
	s.stats.mu.Lock()
	s.stats.fails[op]++
	s.stats.mu.Unlock()
	if n <= maxReported {
		fmt.Printf("%s FAIL %s %q: %s\n", time.Now().Format(time.TimeOnly), op, key, msg)
	} else if n == maxReported+1 {
		fmt.Println("more failures are counted, not printed")
	}
}

func main() {
	var cfg config
	var maxSize, writeRate string
	flag.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:5553", "S3 endpoint, when not starting the server")
	flag.StringVar(&cfg.metricsURL, "metrics", "http://127.0.0.1:5554", "metrics listener, when not starting the server")
	flag.StringVar(&cfg.accessKey, "access-key", envOr("STUPID_RW_ACCESS_KEY", "AKIAIOSFODNN7EXAMPLE"), "read-write access key")
	flag.StringVar(&cfg.secretKey, "secret-key", envOr("STUPID_RW_SECRET_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"), "read-write secret key")
	flag.StringVar(&cfg.bucket, "bucket", fmt.Sprintf("soak-%d", time.Now().Unix()), "bucket to create and use")
	flag.StringVar(&cfg.server, "server", "", "server binary to start, on free ports with fresh directories")
	flag.StringVar(&cfg.workdir, "workdir", "", "directory for the started server's data and log, and for goroutine dumps (default: a new temp directory)")
	flag.StringVar(&cfg.dataDir, "data-dir", "", "STUPID_STORAGE_PATH of a server not started by soak, to check for temp files")
	flag.StringVar(&cfg.multipartDir, "multipart-dir", "", "STUPID_MULTIPART_PATH of a server not started by soak, to check for uploads left")
	flag.DurationVar(&cfg.duration, "duration", time.Hour, "how long to run the workload")
	flag.DurationVar(&cfg.interval, "checkpoint", 5*time.Minute, "time between checkpoints")
	flag.IntVar(&cfg.workers, "workers", 8, "concurrent workers")
	flag.IntVar(&cfg.keysPerWorker, "keys", 200, "keys owned by each worker")
	flag.IntVar(&cfg.sharedKeys, "shared-keys", 16, "keys written by all workers")
	flag.StringVar(&maxSize, "max-size", "1M", "largest single PUT, as bytes or with a K or M suffix")
	flag.Float64Var(&cfg.rate, "rate", 1000, "operations per second over all workers, 0 for no limit")
	flag.StringVar(&writeRate, "write-rate", "32M", "bytes per second the server may be made to write, as bytes or with a K or M suffix, 0 for no limit")
	flag.Uint64Var(&cfg.seed, "seed", uint64(time.Now().UnixNano()), "random seed, to repeat a run")
	flag.DurationVar(&cfg.settleTimeout, "settle", 30*time.Second, "how long a checkpoint waits for goroutines to exit")
	flag.Float64Var(&cfg.goroutineSlack, "goroutine-slack", 2, "idle goroutines allowed over the baseline")
	flag.Float64Var(&cfg.fdSlack, "fd-slack", 2, "idle open fds allowed over the baseline")
	flag.Float64Var(&cfg.heapGrowth, "heap-growth", 1.5, "factor the live heap may grow by over the baseline, plus 16 MiB")
	flag.Float64Var(&cfg.heapSlope, "heap-slope", 1, "bytes per operation the live heap may grow by, judged over the later half of the checkpoints, at least 3")
	flag.IntVar(&cfg.multipartShare, "multipart-weight", 2, "weight of multipart uploads in the operation mix, out of about 100 for all operations, to bring out leaks in the multipart code")
	flag.BoolVar(&cfg.keep, "keep", false, "keep the bucket and the started server's directory")
	flag.Parse()

	var err error
	if cfg.maxSize, err = parseSize(maxSize); err != nil || cfg.maxSize < 1<<20 {
		fatalf("-max-size must be at least 1M")
	}
	if cfg.writeRate, err = parseSize(writeRate); err != nil || cfg.writeRate < 0 {
		fatalf("-write-rate must be a size, or 0")
	}
	if cfg.multipartShare < 0 {
		fatalf("-multipart-weight must not be negative")
	}
	setWeight("multipart", cfg.multipartShare)
	if cfg.keysPerWorker < 2 {
		fatalf("-keys must be at least 2")
	}
	os.Exit(run(cfg))
}

func run(cfg config) int {
	if cfg.workdir == "" {
		dir, err := os.MkdirTemp("", "sss-soak-*")
		if err != nil {
			fatalf("%v", err)
		}
		cfg.workdir = dir
	} else if err := os.MkdirAll(cfg.workdir, 0o700); err != nil {
		fatalf("%v", err)
	}

	s := &soak{
		stats:        opStats{counts: map[string]int64{}, fails: map[string]int64{}},
		opLimit:      newLimiter(cfg.rate),
		writeLimit:   newLimiter(float64(cfg.writeRate)),
		serverExited: make(chan error, 1),
	}

	var srv *exec.Cmd
	if cfg.server != "" {
		var err error
		if srv, err = startServer(&cfg, s.serverExited); err != nil {
			fatalf("starting server: %v", err)
		}
		defer func() {
			if srv != nil {
				_ = srv.Process.Kill()
			}
		}()
	}
	s.cfg = cfg
	s.setupClients()

	fmt.Printf("soak: endpoint %s, bucket %s, seed %d, %d workers x %d keys, %s, checkpoint every %s\n",
		cfg.endpoint, cfg.bucket, cfg.seed, cfg.workers, cfg.keysPerWorker, cfg.duration, cfg.interval)
	fmt.Printf("soak: workdir %s\n", cfg.workdir)
	fmt.Printf("soak: rate %s ops/s, write rate %s/s, max size %s\n",
		limitString(cfg.rate, func(v float64) string { return fmt.Sprintf("%.0f", v) }),
		limitString(float64(cfg.writeRate), mib), mib(float64(cfg.maxSize)))

	ctx := context.Background()
	if _, err := s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &cfg.bucket}); err != nil {
		fatalf("creating bucket %s: %v", cfg.bucket, err)
	}

	for i := range cfg.workers {
		s.workers = append(s.workers, newWorker(s, i))
	}

	// Fill the store, then take the baseline
	start := time.Now()
	var wg sync.WaitGroup
	for _, w := range s.workers {
		wg.Go(w.fill)
	}
	wg.Wait()
	fmt.Printf("soak: filled %d keys in %s\n", cfg.workers*cfg.keysPerWorker, time.Since(start).Round(time.Millisecond))
	s.checkpoint("baseline")

	stop := make(chan struct{})
	for _, w := range s.workers {
		wg.Go(func() { w.run(stop) })
	}

	interrupted := make(chan os.Signal, 1)
	signal.Notify(interrupted, os.Interrupt, syscall.SIGTERM)
	end := time.After(cfg.duration)
	checkpoints := time.NewTicker(cfg.interval)
	progress := time.NewTicker(30 * time.Second)
	lastOps, lastWritten, lastAt := s.stats.total(), s.stats.written.Load(), time.Now()
	n := 0
loop:
	for {
		select {
		case <-end:
			break loop
		case <-interrupted:
			fmt.Println("soak: interrupted, finishing")
			break loop
		case err := <-s.serverExited:
			s.fail("server", "exit", fmt.Sprintf("server exited during the run: %v", err))
			srv = nil
			close(stop)
			wg.Wait()
			return s.report()
		case <-checkpoints.C:
			n++
			s.checkpoint(fmt.Sprintf("cp%d", n))
		case <-progress.C:
			ops := s.stats.total()
			written := s.stats.written.Load()
			elapsed := time.Since(lastAt).Seconds()
			fmt.Printf("%s progress ops=%d (%.0f/s) sent=%s received=%s written=%s (%s/s) failures=%d\n",
				time.Now().Format(time.TimeOnly), ops, float64(ops-lastOps)/elapsed,
				mib(float64(s.stats.sent.Load())), mib(float64(s.stats.received.Load())),
				mib(float64(written)), mib(float64(written-lastWritten)/elapsed), s.failures.Load())
			lastOps, lastWritten, lastAt = ops, written, time.Now()
		}
	}
	close(stop)
	wg.Wait()
	s.checkpoint("final")
	s.checkHeap()

	if !cfg.keep {
		s.deleteBucket()
	}
	if srv != nil {
		s.stopServer(srv)
		srv = nil
		if !cfg.keep {
			_ = os.RemoveAll(filepath.Join(cfg.workdir, "data"))
			_ = os.RemoveAll(filepath.Join(cfg.workdir, "multipart"))
		}
	}
	return s.report()
}

func (s *soak) setupClients() {
	s.transport = &http.Transport{
		MaxIdleConns:        4 * s.cfg.workers,
		MaxIdleConnsPerHost: 4 * s.cfg.workers,
		IdleConnTimeout:     90 * time.Second,
	}
	s.httpClient = &http.Client{Transport: s.transport}
	s.rawClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	s.monitorClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 30 * time.Second}
	s.creds = aws.Credentials{AccessKeyID: s.cfg.accessKey, SecretAccessKey: s.cfg.secretKey}
	s.signer = v4.NewSigner()
	s.client = s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(s.cfg.endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(s.cfg.accessKey, s.cfg.secretKey, ""),
		UsePathStyle: true,
		HTTPClient:   s.httpClient,
		// A retry would hide a failed request
		RetryMaxAttempts: 1,
	})
	s.presign = s3.NewPresignClient(s.client)
}

// startServer starts the server binary on free ports, with its data and log
// in cfg.workdir, and points cfg at it
func startServer(cfg *config, exited chan<- error) (*exec.Cmd, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	metricsPort, err := freePort()
	if err != nil {
		return nil, err
	}
	cfg.endpoint = fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg.metricsURL = fmt.Sprintf("http://127.0.0.1:%d", metricsPort)
	cfg.dataDir = filepath.Join(cfg.workdir, "data")
	cfg.multipartDir = filepath.Join(cfg.workdir, "multipart")

	log, err := os.Create(filepath.Join(cfg.workdir, "server.log"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(cfg.server)
	cmd.Stdout, cmd.Stderr = log, log
	cmd.Env = append(os.Environ(),
		"STUPID_HOST=127.0.0.1",
		fmt.Sprintf("STUPID_PORT=%d", port),
		"STUPID_METRICS_ADDRESS="+strings.TrimPrefix(cfg.metricsURL, "http://"),
		"STUPID_PPROF_ENABLED=true",
		"STUPID_STORAGE_PATH="+cfg.dataDir,
		"STUPID_MULTIPART_PATH="+cfg.multipartDir,
		"STUPID_RW_ACCESS_KEY="+cfg.accessKey,
		"STUPID_RW_SECRET_KEY="+cfg.secretKey,
		// Every request is logged at info, which over hours fills the disk
		"STUPID_LOG_LEVEL=warn",
		// The cleanup job would remove what the leak checks look for
		"STUPID_CLEANUP_ENABLED=false",
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	go func() { exited <- cmd.Wait() }()

	client := &http.Client{Timeout: time.Second}
	for range 100 {
		if resp, err := client.Get(cfg.metricsURL + "/metrics"); err == nil {
			_ = resp.Body.Close()
			return cmd, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return nil, errors.New("server did not start, see " + log.Name())
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// stopServer stops the server with SIGTERM, which must make it exit
// cleanly, and checks its log for errors
func (s *soak) stopServer(cmd *exec.Cmd) {
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-s.serverExited:
		if err != nil {
			s.fail("server", "shutdown", fmt.Sprintf("server exited with %v", err))
		}
	case <-time.After(time.Minute):
		s.fail("server", "shutdown", "server did not exit within a minute of SIGTERM")
		_ = cmd.Process.Kill()
	}

	log, err := os.ReadFile(filepath.Join(s.cfg.workdir, "server.log"))
	if err != nil {
		return
	}
	var errorLines []string
	for _, line := range strings.Split(string(log), "\n") {
		if strings.Contains(line, "level=ERROR") {
			errorLines = append(errorLines, line)
		}
	}
	if len(errorLines) > 0 {
		s.fail("server", "log", fmt.Sprintf("%d errors in the server log, the first: %s", len(errorLines), errorLines[0]))
	}
}

// deleteBucket deletes every object and the bucket
func (s *soak) deleteBucket() {
	ctx := context.Background()
	found, err := s.listKeys(ctx, "", 1000)
	if err != nil {
		fmt.Println("soak: cleanup:", err)
		return
	}
	keys := make([]string, 0, len(found))
	for key := range found {
		keys = append(keys, key)
	}
	for batch := range slices.Chunk(keys, 1000) {
		var objects []types.ObjectIdentifier
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
		}
		if _, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &s.cfg.bucket, Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		}); err != nil {
			fmt.Println("soak: cleanup:", err)
			return
		}
	}
	if _, err := s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &s.cfg.bucket}); err != nil {
		fmt.Println("soak: cleanup:", err)
	}
}

// report prints the operation counts and the result, and returns the exit
// code: 0 if nothing failed
func (s *soak) report() int {
	fmt.Println()
	fmt.Printf("%-16s %12s %8s\n", "operation", "count", "failed")
	names := make([]string, 0, len(s.stats.counts))
	for name := range s.stats.counts {
		names = append(names, name)
	}
	for name := range s.stats.fails {
		if _, ok := s.stats.counts[name]; !ok {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Printf("%-16s %12d %8d\n", name, s.stats.counts[name], s.stats.fails[name])
	}
	if len(s.samples) > 0 {
		base, last := s.samples[0], s.samples[len(s.samples)-1]
		fmt.Printf("\nidle server, baseline -> final: goroutines %.0f -> %.0f, fds %.0f -> %.0f, heap %s -> %s, rss %s -> %s\n",
			base.goroutines, last.goroutines, base.openFDs, last.openFDs, mib(base.heap), mib(last.heap), mib(base.rss), mib(last.rss))
	}
	if n := s.failures.Load(); n > 0 {
		fmt.Printf("\nSOAK FAILED: %d failures\n", n)
		return 1
	}
	fmt.Println("\nSOAK PASSED")
	return 0
}

func parseSize(v string) (int64, error) {
	mult := int64(1)
	switch {
	case strings.HasSuffix(v, "K"):
		mult, v = 1<<10, strings.TrimSuffix(v, "K")
	case strings.HasSuffix(v, "M"):
		mult, v = 1<<20, strings.TrimSuffix(v, "M")
	}
	var n int64
	if _, err := fmt.Sscan(v, &n); err != nil {
		return 0, err
	}
	return n * mult, nil
}

func limitString(v float64, format func(float64) string) string {
	if v <= 0 {
		return "unlimited"
	}
	return format(v)
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "soak: "+format+"\n", args...)
	os.Exit(2)
}
