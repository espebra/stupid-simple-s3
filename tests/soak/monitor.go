package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// sample is the state of the server at a checkpoint, with no requests in
// flight and the client's idle connections closed
type sample struct {
	at         time.Time
	ops        int64
	goroutines float64
	openFDs    float64
	// heap is the live heap after a garbage collection, or the current
	// heap if pprof is not enabled to force one
	heap float64
	rss  float64
	// tempFiles are files in the buckets' tmp directories, and uploads
	// the entries in the multipart directory. Both are -1 if the
	// directories are not known.
	tempFiles int
	uploads   int
}

// scrape returns the unlabelled metrics of the server
func (s *soak) scrape() (map[string]float64, error) {
	resp, err := s.monitorClient.Get(s.cfg.metricsURL + "/metrics")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics: status %d", resp.StatusCode)
	}
	metrics := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || line[0] == '#' || strings.Contains(line, "{") {
			continue
		}
		name, value, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if v, err := strconv.ParseFloat(value, 64); err == nil {
			metrics[name] = v
		}
	}
	return metrics, sc.Err()
}

// forceGC makes the server collect garbage, through the pprof heap
// profile, and reports whether it could
func (s *soak) forceGC() bool {
	resp, err := s.monitorClient.Get(s.cfg.metricsURL + "/debug/pprof/heap?gc=1")
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// countDisk counts the files in the buckets' tmp directories and the
// uploads in the multipart directory
func (s *soak) countDisk() (tempFiles, uploads int) {
	if s.cfg.dataDir == "" || s.cfg.multipartDir == "" {
		return -1, -1
	}
	tmpDirs, _ := filepath.Glob(filepath.Join(s.cfg.dataDir, "buckets", "*", "tmp"))
	for _, dir := range tmpDirs {
		_ = filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				tempFiles++
			}
			return nil
		})
	}
	entries, _ := os.ReadDir(s.cfg.multipartDir)
	return tempFiles, len(entries)
}

// checkpoint stops the workers, checks every object, waits for the server
// to go quiet, and takes a sample. The first checkpoint is the baseline the
// others are compared with.
func (s *soak) checkpoint(name string) {
	s.gate.Lock()
	defer s.gate.Unlock()
	start := time.Now()

	var wg sync.WaitGroup
	for _, w := range s.workers {
		wg.Go(w.sweep)
	}
	for i := range s.cfg.sharedKeys {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		s.verifyShared(ctx, "sweep", fmt.Sprintf("shared/%03d", i))
		cancel()
	}
	wg.Wait()
	sweepTime := time.Since(start)

	smp, err := s.quietSample()
	if err != nil {
		s.fail("checkpoint", name, err.Error())
		return
	}
	s.samples = append(s.samples, smp)
	base := s.samples[0]

	fmt.Printf("%s checkpoint %-8s ops=%-9d goroutines=%-4.0f fds=%-4.0f heap=%-7s rss=%-7s tmp-files=%d uploads=%d sweep=%s\n",
		smp.at.Format(time.TimeOnly), name, smp.ops, smp.goroutines, smp.openFDs,
		mib(smp.heap), mib(smp.rss), smp.tempFiles, smp.uploads, sweepTime.Round(time.Millisecond))

	if smp.tempFiles > 0 {
		s.fail("leak", name, fmt.Sprintf("%d temp files left in the buckets' tmp directories", smp.tempFiles))
	}
	if smp.uploads > 0 {
		s.fail("leak", name, fmt.Sprintf("%d entries left in the multipart directory", smp.uploads))
	}
	if len(s.samples) == 1 {
		return
	}
	if smp.goroutines > base.goroutines+s.cfg.goroutineSlack {
		s.fail("leak", name, fmt.Sprintf("%.0f goroutines when idle, baseline %.0f", smp.goroutines, base.goroutines))
		s.dumpGoroutines(name)
	}
	if smp.openFDs > base.openFDs+s.cfg.fdSlack {
		s.fail("leak", name, fmt.Sprintf("%.0f open fds when idle, baseline %.0f", smp.openFDs, base.openFDs))
	}
}

// quietSample waits until the server is idle, and samples it. Goroutines
// of closed connections take a moment to exit, so it polls until the count
// has not changed for a second and is back at the baseline, or
// settleTimeout has passed.
func (s *soak) quietSample() (sample, error) {
	s.transport.CloseIdleConnections()
	deadline := time.Now().Add(s.cfg.settleTimeout)
	prev, stableSince := -1.0, time.Now()
	for {
		m, err := s.scrape()
		if err != nil {
			return sample{}, err
		}
		g := m["go_goroutines"]
		if g != prev {
			prev, stableSince = g, time.Now()
		}
		settled := m["stupid_simple_s3_http_requests_in_flight"] == 0 &&
			time.Since(stableSince) >= time.Second &&
			(len(s.samples) == 0 || g <= s.samples[0].goroutines)
		if settled || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	gc := s.forceGC()
	if !gc && !s.warnedGC {
		fmt.Println("warning: pprof is not enabled on the metrics listener, so the heap is measured without a GC and not checked for growth")
		s.warnedGC = true
	}
	m, err := s.scrape()
	if err != nil {
		return sample{}, err
	}
	// Garbage made between the GC and the scrape, by the scrapes themselves
	// and the server's background work, makes single readings noisy. The
	// lowest of three is closest to the live heap.
	heap := m["go_memstats_heap_alloc_bytes"]
	for range 2 {
		if !gc || !s.forceGC() {
			break
		}
		if again, err := s.scrape(); err == nil {
			heap = min(heap, again["go_memstats_heap_alloc_bytes"])
		}
	}
	tempFiles, uploads := s.countDisk()
	smp := sample{
		at:         time.Now(),
		ops:        s.stats.total(),
		goroutines: m["go_goroutines"],
		openFDs:    m["process_open_fds"],
		heap:       heap,
		rss:        m["process_resident_memory_bytes"],
		tempFiles:  tempFiles,
		uploads:    uploads,
	}
	s.heapChecked = gc
	return smp, nil
}

// dumpGoroutines saves the server's goroutine stacks, to find a leak
func (s *soak) dumpGoroutines(name string) {
	resp, err := s.monitorClient.Get(s.cfg.metricsURL + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return
	}
	path := filepath.Join(s.cfg.workdir, "goroutines-"+name+".txt")
	f, err := os.Create(path)
	if err != nil {
		return
	}
	_, _ = io.Copy(f, resp.Body)
	_ = f.Close()
	fmt.Println("goroutine stacks saved to", path)
}

// checkHeap compares the heap at the last checkpoint with the baseline.
// The store is full from the baseline on, so the live heap must level off.
func (s *soak) checkHeap() {
	if len(s.samples) < 2 || !s.heapChecked {
		return
	}
	base, last := s.samples[0], s.samples[len(s.samples)-1]
	limit := base.heap*s.cfg.heapGrowth + 16<<20
	if last.heap > limit {
		s.fail("leak", "heap", fmt.Sprintf("live heap grew from %s to %s, limit %s", mib(base.heap), mib(last.heap), mib(limit)))
	}
	s.checkHeapTrend()
}

// minTrendSamples is the number of checkpoints the heap's trend is judged
// over, in the later half of the run
const minTrendSamples = 3

// checkHeapTrend fits a line through the live heap against the operation
// count at the checkpoints in the later half of the run. The heap grows
// while the server warms up, by about 0.5 MiB over the first 10000
// operations, which a short run would otherwise take for a leak. A leak
// that keeps a few bytes per operation is a steady slope long before it
// reaches the -heap-growth limit. The run fails if the slope, less twice
// its standard error, is over -heap-slope, so scatter in a short run does
// not fail it.
func (s *soak) checkHeapTrend() {
	after := s.samples[1:]
	pts := after[len(after)/2:]
	if len(pts) < 2 {
		return
	}
	slope, stderr := fitLine(pts)
	last := s.samples[len(s.samples)-1]
	perDay := 0.0
	if elapsed := last.at.Sub(s.samples[0].at).Seconds(); elapsed > 0 {
		perDay = slope * float64(last.ops) / elapsed * 86400
	}
	fmt.Printf("\nheap trend over the last %d checkpoints: %+.1f ± %.1f bytes per operation, %+.1fM per million operations, %+.1fM per day at this rate\n",
		len(pts), slope, stderr, slope*1e6/(1<<20), perDay/(1<<20))
	if len(pts) < minTrendSamples {
		fmt.Printf("heap trend not checked: needs %d checkpoints after the baseline\n", 2*minTrendSamples)
		return
	}
	if slope-2*stderr > s.cfg.heapSlope {
		s.fail("leak", "heap", fmt.Sprintf("live heap grows by %.1f ± %.1f bytes per operation, limit %g", slope, stderr, s.cfg.heapSlope))
	}
}

// fitLine returns the least squares slope of heap against ops, and its
// standard error
func fitLine(pts []sample) (slope, stderr float64) {
	n := float64(len(pts))
	var mx, my float64
	for _, p := range pts {
		mx += float64(p.ops)
		my += p.heap
	}
	mx, my = mx/n, my/n
	var sxx, sxy float64
	for _, p := range pts {
		dx := float64(p.ops) - mx
		sxx += dx * dx
		sxy += dx * (p.heap - my)
	}
	if sxx == 0 {
		return 0, 0
	}
	slope = sxy / sxx
	var res float64
	for _, p := range pts {
		d := p.heap - my - slope*(float64(p.ops)-mx)
		res += d * d
	}
	if len(pts) > 2 {
		stderr = math.Sqrt(res / (n - 2) / sxx)
	}
	return slope, stderr
}

func mib(b float64) string { return fmt.Sprintf("%.1fM", b/(1<<20)) }
