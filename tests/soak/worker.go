package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// opTimeout bounds every request, so that a hung request is reported
// instead of stalling the soak test
const opTimeout = 5 * time.Minute

const minPartSize = 5 << 20

// worker runs operations on the keys it owns, so it knows what every one of
// them must hold, and on the keys shared by all workers, which it can only
// check with verifySelf.
type worker struct {
	s      *soak
	id     int
	rng    *rand.Rand
	prefix string
	keys   []string
	// model is the content of each owned key, or nil if it has none
	model map[string]*content
	// written is the bytes the current operation made the server write,
	// paid for at -write-rate once the operation is done
	written int64
}

// keyTemplates give the owned keys a mix of plain names and names that
// need escaping. Index 0 must stay plain: the raw HTTP operations use it.
var keyTemplates = []string{
	"%splain/%04d",
	"%sdeep/a/b/c/%04d.bin",
	"%swith space/%04d",
	"%sünï+cødé~!/%04d",
}

func newWorker(s *soak, id int) *worker {
	w := &worker{
		s:      s,
		id:     id,
		rng:    rand.New(rand.NewPCG(s.cfg.seed, uint64(id))),
		prefix: fmt.Sprintf("w%03d/", id),
		model:  map[string]*content{},
	}
	for i := range s.cfg.keysPerWorker {
		key := fmt.Sprintf(keyTemplates[i%len(keyTemplates)], w.prefix, i)
		w.keys = append(w.keys, key)
		w.model[key] = nil
	}
	return w
}

type op struct {
	name   string
	weight int
	run    func(w *worker, ctx context.Context)
}

var ops = []op{
	{"put", 20, (*worker).put},
	{"get", 18, (*worker).get},
	{"range-get", 10, (*worker).rangeGet},
	{"head", 6, (*worker).head},
	{"conditional-get", 4, (*worker).conditionalGet},
	{"conditional-put", 5, (*worker).conditionalPut},
	{"delete", 4, (*worker).delete},
	{"delete-objects", 2, (*worker).deleteObjects},
	{"copy", 3, (*worker).copyObject},
	{"list", 3, (*worker).list},
	{"multipart", 2, (*worker).multipart},
	{"chunked-put", 4, (*worker).chunkedPut},
	{"presigned-get", 2, (*worker).presignedGet},
	{"presigned-put", 2, (*worker).presignedPut},
	{"bucket", 1, (*worker).bucketOps},
	{"auth", 1, (*worker).authChecks},
	{"health", 1, (*worker).health},
	{"interrupted-put", 3, (*worker).interruptedPut},
	{"interrupted-get", 3, (*worker).interruptedGet},
	{"tampered-put", 1, (*worker).tamperedPut},
	{"shared-put", 4, (*worker).sharedPut},
	{"shared-get", 6, (*worker).sharedGet},
	{"shared-delete", 1, (*worker).sharedDelete},
}

// setWeight changes the weight of an operation, before the workers start
func setWeight(name string, weight int) {
	for i := range ops {
		if ops[i].name == name {
			ops[i].weight = weight
			return
		}
	}
	panic("unknown operation " + name)
}

func (w *worker) pickOp() *op {
	total := 0
	for i := range ops {
		total += ops[i].weight
	}
	n := w.rng.IntN(total)
	for i := range ops {
		if n < ops[i].weight {
			return &ops[i]
		}
		n -= ops[i].weight
	}
	panic("unreachable")
}

// run performs operations until stop is closed. Each operation holds the
// gate for reading, so that a checkpoint can wait for a quiet server.
func (w *worker) run(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		w.s.opLimit.wait(1, stop)
		o := w.pickOp()
		w.s.gate.RLock()
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		o.run(w, ctx)
		cancel()
		w.s.gate.RUnlock()
		w.s.stats.count(o.name)
		w.payWrites(stop)
	}
}

// wrote records n bytes written by the server for the current operation
func (w *worker) wrote(n int64) {
	w.written += n
	w.s.stats.written.Add(n)
}

// payWrites waits for the bytes the last operation wrote, outside the gate,
// so that a checkpoint does not wait for the limit
func (w *worker) payWrites(stop <-chan struct{}) {
	w.s.writeLimit.wait(float64(w.written), stop)
	w.written = 0
}

func (w *worker) fail(op, key, format string, args ...any) {
	w.s.fail(op, key, fmt.Sprintf(format, args...))
}

// resync puts a key the worker no longer knows the content of, after a
// failed operation, back in a known state by deleting it
func (w *worker) resync(ctx context.Context, key string) {
	if _, err := w.s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &w.s.cfg.bucket, Key: &key}); err != nil {
		w.fail("resync", key, "delete: %v", err)
	}
	w.model[key] = nil
}

func (w *worker) pickKey() string { return w.keys[w.rng.IntN(len(w.keys))] }

// pickPlainKey returns a key that needs no escaping in a URL path
func (w *worker) pickPlainKey() string {
	return w.keys[w.rng.IntN((len(w.keys)+len(keyTemplates)-1)/len(keyTemplates))*len(keyTemplates)]
}

// pickStored returns an owned key that has content, or "" if none has
func (w *worker) pickStored() string {
	for range 8 {
		if key := w.pickKey(); w.model[key] != nil {
			return key
		}
	}
	return ""
}

// size returns an object size: mostly small, sometimes up to maxSize
func (w *worker) size() int64 {
	switch n := w.rng.IntN(100); {
	case n < 3:
		return 0
	case n < 35:
		return 1 + w.rng.Int64N(1<<10)
	case n < 65:
		return 1<<10 + w.rng.Int64N(63<<10)
	case n < 92:
		return 64<<10 + w.rng.Int64N(min(960<<10, w.s.cfg.maxSize))
	default:
		return 1<<20 + w.rng.Int64N(max(w.s.cfg.maxSize-1<<20, 1))
	}
}

func (w *worker) newContent(key string, size int64) *content {
	return &content{genKey: key, seed: w.rng.Uint64(), size: size, contentType: defaultContentType}
}

const defaultContentType = "application/octet-stream"

var contentTypes = []string{defaultContentType, "text/plain; charset=utf-8", "image/png", "application/x-soak"}

// setAttributes gives c a random Content-Type and user metadata, either of
// which may be left out, and returns the Content-Type to send: "" for none
func (w *worker) setAttributes(c *content) string {
	sent := ""
	c.contentType, c.metadata = defaultContentType, nil
	if w.rng.IntN(2) == 0 {
		sent = contentTypes[w.rng.IntN(len(contentTypes))]
		c.contentType = sent
	}
	if w.rng.IntN(2) == 0 {
		c.metadata = map[string]string{"worker": strconv.Itoa(w.id), "seed": strconv.FormatUint(c.seed, 16)}
		if w.rng.IntN(2) == 0 {
			c.metadata["note-2"] = "a b=c; d,e/f (g) \"h\""
		}
	}
	return sent
}

// checkAttributes compares the Content-Type and user metadata of a
// response with what the object was stored with
func (w *worker) checkAttributes(op, key string, c *content, contentType *string, metadata map[string]string) {
	if got := aws.ToString(contentType); got != c.contentType {
		w.fail(op, key, "Content-Type %q, want %q", got, c.contentType)
	}
	got := make(map[string]string, len(metadata))
	for k, v := range metadata {
		got[strings.ToLower(k)] = v
	}
	if !maps.Equal(got, c.metadata) {
		w.fail(op, key, "metadata %v, want %v", got, c.metadata)
	}
}

func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// expectError checks that err is an error response with the given status
// and, if code is not empty, error code
func expectError(err error, status int, code string) error {
	if err == nil {
		return fmt.Errorf("succeeded, want %d %s", status, code)
	}
	if httpStatus(err) != status || (code != "" && errorCode(err) != code) {
		return fmt.Errorf("got %v, want %d %s", err, status, code)
	}
	return nil
}

// putContent uploads c to key, checks the ETag and records the content
func (w *worker) putContent(ctx context.Context, op, key string, c *content, mutate func(*s3.PutObjectInput)) error {
	data := c.bytes()
	in := &s3.PutObjectInput{
		Bucket:        &w.s.cfg.bucket,
		Key:           &key,
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(c.size),
	}
	if w.rng.IntN(2) == 0 {
		in.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	if ct := w.setAttributes(c); ct != "" {
		in.ContentType = &ct
	}
	in.Metadata = c.metadata
	if mutate != nil {
		mutate(in)
	}
	// Counted whatever the outcome: a refused conditional write is still
	// written to a temp file first
	w.wrote(c.size)
	out, err := w.s.client.PutObject(ctx, in)
	if err != nil {
		return err
	}
	w.s.stats.sent.Add(c.size)
	if want := singleETag(data); aws.ToString(out.ETag) != want {
		w.fail(op, key, "ETag %s, want %s", aws.ToString(out.ETag), want)
	}
	c.etag = singleETag(data)
	w.model[key] = c
	return nil
}

func (w *worker) put(ctx context.Context) {
	key := w.pickKey()
	if err := w.putContent(ctx, "put", key, w.newContent(key, w.size()), nil); err != nil {
		w.fail("put", key, "%v", err)
		w.resync(ctx, key)
	}
}

// verifyKey checks that key holds what the model says, with a GET
func (w *worker) verifyKey(ctx context.Context, op, key string) {
	w.verifyKeyWith(ctx, w.s.client, op, key)
}

// verifyKeyWith is verifyKey with the given client
func (w *worker) verifyKeyWith(ctx context.Context, client *s3.Client, op, key string) {
	c := w.model[key]
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: &w.s.cfg.bucket, Key: &key})
	if c == nil {
		if err := expectError(err, http.StatusNotFound, "NoSuchKey"); err != nil {
			w.fail(op, key, "deleted key: %v", err)
		}
		if out != nil {
			_ = out.Body.Close()
		}
		return
	}
	if err != nil {
		w.fail(op, key, "%v", err)
		return
	}
	defer func() { _ = out.Body.Close() }()
	if aws.ToString(out.ETag) != c.etag {
		w.fail(op, key, "ETag %s, want %s", aws.ToString(out.ETag), c.etag)
	}
	if aws.ToInt64(out.ContentLength) != c.size {
		w.fail(op, key, "Content-Length %d, want %d", aws.ToInt64(out.ContentLength), c.size)
	}
	w.checkAttributes(op, key, c, out.ContentType, out.Metadata)
	if err := c.verifyRange(out.Body, 0, c.size); err != nil {
		w.fail(op, key, "%v", err)
	}
	w.s.stats.received.Add(c.size)
}

func (w *worker) get(ctx context.Context) { w.verifyKey(ctx, "get", w.pickKey()) }

func (w *worker) rangeGet(ctx context.Context) {
	key := w.pickStored()
	if key == "" {
		return
	}
	c := w.model[key]
	size := c.size

	// header, and the range it asks for; satisfiable is false if the
	// server must answer 416
	var header string
	var start, end int64
	var satisfiable bool
	switch w.rng.IntN(3) {
	case 0:
		start = w.rng.Int64N(size + 10)
		e := start + w.rng.Int64N(size+1)
		header = fmt.Sprintf("bytes=%d-%d", start, e)
		end = min(e, size-1)
		satisfiable = start < size
	case 1:
		start = w.rng.Int64N(size + 10)
		header = fmt.Sprintf("bytes=%d-", start)
		end = size - 1
		satisfiable = start < size
	default:
		n := 1 + w.rng.Int64N(size+10)
		header = fmt.Sprintf("bytes=-%d", n)
		start, end = max(size-n, 0), size-1
		satisfiable = size > 0
	}

	out, err := w.s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &w.s.cfg.bucket, Key: &key, Range: &header})
	if !satisfiable {
		if err := expectError(err, http.StatusRequestedRangeNotSatisfiable, "InvalidRange"); err != nil {
			w.fail("range-get", key, "%s of %d bytes: %v", header, size, err)
		}
		if out != nil {
			_ = out.Body.Close()
		}
		return
	}
	if err != nil {
		w.fail("range-get", key, "%s of %d bytes: %v", header, size, err)
		return
	}
	defer func() { _ = out.Body.Close() }()
	if want := fmt.Sprintf("bytes %d-%d/%d", start, end, size); aws.ToString(out.ContentRange) != want {
		w.fail("range-get", key, "%s: Content-Range %q, want %q", header, aws.ToString(out.ContentRange), want)
		return
	}
	if err := c.verifyRange(out.Body, start, end-start+1); err != nil {
		w.fail("range-get", key, "%s: %v", header, err)
	}
	w.s.stats.received.Add(end - start + 1)
}

func (w *worker) head(ctx context.Context) {
	key := w.pickKey()
	c := w.model[key]
	out, err := w.s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &w.s.cfg.bucket, Key: &key, ChecksumMode: types.ChecksumModeEnabled})
	if c == nil {
		if err := expectError(err, http.StatusNotFound, ""); err != nil {
			w.fail("head", key, "deleted key: %v", err)
		}
		return
	}
	if err != nil {
		w.fail("head", key, "%v", err)
		return
	}
	if aws.ToString(out.ETag) != c.etag || aws.ToInt64(out.ContentLength) != c.size {
		w.fail("head", key, "ETag %s size %d, want %s size %d", aws.ToString(out.ETag), aws.ToInt64(out.ContentLength), c.etag, c.size)
	}
	if got, want := aws.ToString(out.ChecksumSHA256), etagChecksum(c.etag); got != want {
		w.fail("head", key, "x-amz-checksum-sha256 %s, want %s", got, want)
	}
	w.checkAttributes("head", key, c, out.ContentType, out.Metadata)
}

// conditionalGet sends a GET or HEAD with a conditional header, sometimes
// with a range, and checks that it is answered with the object, 304 or 412
func (w *worker) conditionalGet(ctx context.Context) {
	key := w.pickStored()
	if key == "" {
		return
	}
	c := w.model[key]
	head, err := w.s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &w.s.cfg.bucket, Key: &key})
	if err != nil {
		w.fail("conditional-get", key, "head: %v", err)
		return
	}
	modified := aws.ToTime(head.LastModified)
	before := modified.Add(-time.Second)

	in := &s3.GetObjectInput{Bucket: &w.s.cfg.bucket, Key: &key}
	var cond string
	want := http.StatusOK
	switch w.rng.IntN(10) {
	case 0:
		cond, in.IfMatch = "If-Match: current", aws.String(c.etag)
	case 1:
		cond, in.IfMatch = "If-Match: list", aws.String(`"sha256-other", `+c.etag)
	case 2:
		cond, in.IfMatch, want = "If-Match: other", aws.String(`"sha256-other"`), http.StatusPreconditionFailed
	case 3:
		cond, in.IfNoneMatch, want = "If-None-Match: current", aws.String(c.etag), http.StatusNotModified
	case 4:
		cond, in.IfNoneMatch, want = "If-None-Match: weak", aws.String("W/"+c.etag), http.StatusNotModified
	case 5:
		cond, in.IfNoneMatch = "If-None-Match: other", aws.String(`"sha256-other"`)
	case 6:
		cond, in.IfModifiedSince, want = "If-Modified-Since: Last-Modified", &modified, http.StatusNotModified
	case 7:
		cond, in.IfModifiedSince = "If-Modified-Since: before", &before
	case 8:
		cond, in.IfUnmodifiedSince = "If-Unmodified-Since: Last-Modified", &modified
	default:
		cond, in.IfUnmodifiedSince, want = "If-Unmodified-Since: before", &before, http.StatusPreconditionFailed
	}

	if w.rng.IntN(2) == 0 {
		out, err := w.s.client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: in.Bucket, Key: in.Key, IfMatch: in.IfMatch, IfNoneMatch: in.IfNoneMatch,
			IfModifiedSince: in.IfModifiedSince, IfUnmodifiedSince: in.IfUnmodifiedSince,
		})
		if want != http.StatusOK {
			if err := expectError(err, want, ""); err != nil {
				w.fail("conditional-get", key, "HEAD %s: %v", cond, err)
			}
			return
		}
		if err != nil {
			w.fail("conditional-get", key, "HEAD %s: %v", cond, err)
		} else if aws.ToString(out.ETag) != c.etag {
			w.fail("conditional-get", key, "HEAD %s: ETag %s, want %s", cond, aws.ToString(out.ETag), c.etag)
		}
		return
	}

	// The conditions are evaluated before the range
	start, n := int64(0), c.size
	if c.size > 0 && w.rng.IntN(2) == 0 {
		start = w.rng.Int64N(c.size)
		n = 1 + w.rng.Int64N(c.size-start)
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", start, start+n-1))
	}
	out, err := w.s.client.GetObject(ctx, in)
	if want != http.StatusOK {
		code := "PreconditionFailed"
		if want == http.StatusNotModified {
			code = ""
		}
		if err := expectError(err, want, code); err != nil {
			w.fail("conditional-get", key, "%s: %v", cond, err)
		}
		if out != nil {
			_ = out.Body.Close()
		}
		return
	}
	if err != nil {
		w.fail("conditional-get", key, "%s: %v", cond, err)
		return
	}
	defer func() { _ = out.Body.Close() }()
	if err := c.verifyRange(out.Body, start, n); err != nil {
		w.fail("conditional-get", key, "%s: %v", cond, err)
	}
	w.s.stats.received.Add(n)
}

func (w *worker) conditionalPut(ctx context.Context) {
	key := w.pickKey()
	current := w.model[key]
	c := w.newContent(key, w.size())

	var cond string
	var mutate func(*s3.PutObjectInput)
	wantStatus, wantCode := http.StatusOK, ""
	switch w.rng.IntN(7) {
	case 6:
		// S3 does not support writes conditional on another ETag
		cond = "If-None-Match: ETag"
		mutate = func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String(`"sha256-other"`) }
		wantStatus, wantCode = http.StatusNotImplemented, "NotImplemented"
	case 0, 1:
		cond = "If-None-Match: *"
		mutate = func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") }
		if current != nil {
			wantStatus, wantCode = http.StatusPreconditionFailed, "PreconditionFailed"
		}
	case 2, 3:
		cond = "If-Match: current"
		etag := `"sha256-none"`
		if current != nil {
			etag = current.etag
		}
		mutate = func(in *s3.PutObjectInput) { in.IfMatch = aws.String(etag) }
		if current == nil {
			wantStatus, wantCode = http.StatusNotFound, "NoSuchKey"
		}
	default:
		cond = "If-Match: other"
		mutate = func(in *s3.PutObjectInput) { in.IfMatch = aws.String(`"sha256-other"`) }
		wantStatus, wantCode = http.StatusPreconditionFailed, "PreconditionFailed"
		if current == nil {
			wantStatus, wantCode = http.StatusNotFound, "NoSuchKey"
		}
	}

	err := w.putContent(ctx, "conditional-put", key, c, mutate)
	if wantStatus == http.StatusOK {
		if err != nil {
			w.fail("conditional-put", key, "%s: %v", cond, err)
			w.resync(ctx, key)
		}
		return
	}
	if err := expectError(err, wantStatus, wantCode); err != nil {
		w.fail("conditional-put", key, "%s: %v", cond, err)
		w.resync(ctx, key)
	}
}

func (w *worker) delete(ctx context.Context) {
	key := w.pickKey()
	if _, err := w.s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &w.s.cfg.bucket, Key: &key}); err != nil {
		w.fail("delete", key, "%v", err)
		return
	}
	w.model[key] = nil
}

func (w *worker) deleteObjects(ctx context.Context) {
	n := 1 + w.rng.IntN(8)
	var keys []string
	var objects []types.ObjectIdentifier
	for range n {
		key := w.pickKey()
		if slices.Contains(keys, key) {
			continue
		}
		keys = append(keys, key)
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(key)})
	}
	quiet := w.rng.IntN(2) == 0
	out, err := w.s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: &w.s.cfg.bucket,
		Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(quiet)},
	})
	if err != nil {
		w.fail("delete-objects", keys[0], "%v", err)
		for _, key := range keys {
			w.resync(ctx, key)
		}
		return
	}
	for _, e := range out.Errors {
		w.fail("delete-objects", aws.ToString(e.Key), "%s: %s", aws.ToString(e.Code), aws.ToString(e.Message))
	}
	if !quiet && len(out.Deleted) != len(keys) {
		w.fail("delete-objects", keys[0], "%d keys deleted, want %d", len(out.Deleted), len(keys))
	}
	for _, key := range keys {
		w.model[key] = nil
	}
}

func (w *worker) copyObject(ctx context.Context) {
	src := w.pickStored()
	dst := w.pickKey()
	if src == "" || src == dst {
		return
	}
	c := w.model[src]
	in := &s3.CopyObjectInput{
		Bucket:     &w.s.cfg.bucket,
		Key:        &dst,
		CopySource: aws.String(url.PathEscape(w.s.cfg.bucket + "/" + src)),
	}
	// The copy keeps the source's Content-Type and metadata, unless they
	// are replaced
	copied := *c
	switch w.rng.IntN(3) {
	case 0:
		in.MetadataDirective = types.MetadataDirectiveReplace
		if ct := w.setAttributes(&copied); ct != "" {
			in.ContentType = &ct
		}
		in.Metadata = copied.metadata
	case 1:
		in.MetadataDirective = types.MetadataDirectiveCopy
	}
	out, err := w.s.client.CopyObject(ctx, in)
	if err != nil {
		w.fail("copy", dst, "from %s: %v", src, err)
		w.resync(ctx, dst)
		return
	}
	// A copy is stored as one part, so a copy of a multipart object has
	// another ETag than its source, as in S3
	w.wrote(c.size)
	copied.etag = singleETag(c.bytes())
	var etag string
	if out.CopyObjectResult != nil {
		etag = aws.ToString(out.CopyObjectResult.ETag)
	}
	if etag != copied.etag {
		w.fail("copy", dst, "from %s: ETag %s, want %s", src, etag, copied.etag)
	}
	w.model[dst] = &copied
}

// listing is what a listing returned over all its pages
type listing struct {
	objects  map[string]types.Object
	prefixes map[string]bool
}

// listAll lists prefix in pages of pageSize, with ListObjectsV2 or, if v1,
// ListObjects. It checks that every page lists its keys and common
// prefixes in order, after those of the pages before, and none twice.
func (s *soak) listAll(ctx context.Context, v1 bool, prefix, delimiter, startAfter string, pageSize int32) (*listing, error) {
	l := &listing{objects: map[string]types.Object{}, prefixes: map[string]bool{}}
	// last is the greatest key or common prefix of the pages so far
	var last, marker string
	var token *string
	for page := 0; ; page++ {
		if page > 100000 {
			return nil, errors.New("listing does not end")
		}
		var contents []types.Object
		var prefixes []types.CommonPrefix
		var truncated bool
		if v1 {
			if page == 0 {
				marker = startAfter
			}
			out, err := s.client.ListObjects(ctx, &s3.ListObjectsInput{
				Bucket: &s.cfg.bucket, Prefix: &prefix, Delimiter: optional(delimiter),
				Marker: optional(marker), MaxKeys: aws.Int32(pageSize),
			})
			if err != nil {
				return nil, err
			}
			contents, prefixes, truncated = out.Contents, out.CommonPrefixes, aws.ToBool(out.IsTruncated)
			// Without a delimiter there is no NextMarker: the next page
			// starts after the last key
			marker = aws.ToString(out.NextMarker)
			if marker == "" && delimiter == "" && len(contents) > 0 {
				marker = aws.ToString(contents[len(contents)-1].Key)
			}
			if truncated && marker == "" {
				return nil, errors.New("truncated listing without a marker to continue from")
			}
		} else {
			in := &s3.ListObjectsV2Input{
				Bucket: &s.cfg.bucket, Prefix: &prefix, Delimiter: optional(delimiter),
				MaxKeys: aws.Int32(pageSize), ContinuationToken: token,
			}
			if page == 0 {
				in.StartAfter = optional(startAfter)
			}
			out, err := s.client.ListObjectsV2(ctx, in)
			if err != nil {
				return nil, err
			}
			contents, prefixes, truncated = out.Contents, out.CommonPrefixes, aws.ToBool(out.IsTruncated)
			if n := len(contents) + len(prefixes); aws.ToInt32(out.KeyCount) != int32(n) {
				return nil, fmt.Errorf("KeyCount %d, with %d keys and common prefixes", aws.ToInt32(out.KeyCount), n)
			}
			token = out.NextContinuationToken
			if truncated && token == nil {
				return nil, errors.New("truncated listing without a continuation token")
			}
		}
		if n := len(contents) + len(prefixes); n > int(pageSize) || (truncated && n == 0) {
			return nil, fmt.Errorf("page of %d keys and common prefixes, max-keys %d, truncated %v", n, pageSize, truncated)
		}

		pageLast := last
		prev := last
		for _, o := range contents {
			key := aws.ToString(o.Key)
			if key <= prev || key <= startAfter {
				return nil, fmt.Errorf("key %q listed after %q", key, max(prev, startAfter))
			}
			prev = key
			pageLast = max(pageLast, key)
			l.objects[key] = o
		}
		prev = last
		for _, cp := range prefixes {
			p := aws.ToString(cp.Prefix)
			if p <= prev || l.prefixes[p] {
				return nil, fmt.Errorf("common prefix %q listed after %q", p, prev)
			}
			prev = p
			pageLast = max(pageLast, p)
			l.prefixes[p] = true
		}
		last = pageLast
		if !truncated {
			return l, nil
		}
	}
}

func optional(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

// listKeys lists the keys under prefix, in pages of pageSize
func (s *soak) listKeys(ctx context.Context, prefix string, pageSize int32) (map[string]types.Object, error) {
	l, err := s.listAll(ctx, false, prefix, "", "", pageSize)
	if err != nil {
		return nil, err
	}
	return l.objects, nil
}

// expectListing returns the keys and common prefixes a listing of the
// worker's keys must give
func (w *worker) expectListing(prefix, delimiter, startAfter string) (map[string]*content, map[string]bool) {
	objects, prefixes := map[string]*content{}, map[string]bool{}
	for _, key := range w.keys {
		c := w.model[key]
		if c == nil || !strings.HasPrefix(key, prefix) || key <= startAfter {
			continue
		}
		if delimiter != "" {
			if i := strings.Index(key[len(prefix):], delimiter); i >= 0 {
				prefixes[key[:len(prefix)+i+len(delimiter)]] = true
				continue
			}
		}
		objects[key] = c
	}
	return objects, prefixes
}

// compareListing checks a listing of the worker's keys against the model
func (w *worker) compareListing(op string, want map[string]*content, found map[string]types.Object) {
	for key, c := range want {
		o, ok := found[key]
		switch {
		case !ok:
			w.fail(op, key, "key is not listed")
		case aws.ToString(o.ETag) != c.etag || aws.ToInt64(o.Size) != c.size:
			w.fail(op, key, "listed with ETag %s size %d, want %s size %d", aws.ToString(o.ETag), aws.ToInt64(o.Size), c.etag, c.size)
		}
	}
	for key := range found {
		if _, ok := want[key]; !ok {
			w.fail(op, key, "key is listed, deleted or not under the prefix")
		}
	}
}

// listPrefixes are the prefixes, below the worker's, that list tries
var listPrefixes = []string{"", "plain/", "plain/00", "deep/", "deep/a/b/", "with space/", "ünï"}

func (w *worker) list(ctx context.Context) {
	prefix := w.prefix + listPrefixes[w.rng.IntN(len(listPrefixes))]
	delimiter, startAfter := "", ""
	if w.rng.IntN(2) == 0 {
		delimiter = "/"
	}
	if w.rng.IntN(3) == 0 {
		if key := w.pickKey(); strings.HasPrefix(key, prefix) {
			startAfter = key
		}
	}
	pageSize := 1 + w.rng.Int32N(1000)
	if w.rng.IntN(2) == 0 {
		pageSize = 1 + w.rng.Int32N(10)
	}
	v1 := w.rng.IntN(3) == 0
	desc := fmt.Sprintf("v1 %v, delimiter %q, start after %q, max-keys %d", v1, delimiter, startAfter, pageSize)

	l, err := w.s.listAll(ctx, v1, prefix, delimiter, startAfter, pageSize)
	if err != nil {
		w.fail("list", prefix, "%s: %v", desc, err)
		return
	}
	objects, prefixes := w.expectListing(prefix, delimiter, startAfter)
	w.compareListing("list", objects, l.objects)
	if !maps.Equal(l.prefixes, prefixes) {
		w.fail("list", prefix, "%s: common prefixes %v, want %v", desc, slices.Sorted(maps.Keys(l.prefixes)), slices.Sorted(maps.Keys(prefixes)))
	}
}

func (w *worker) multipart(ctx context.Context) {
	key := w.pickKey()
	// Every part but the last must be at least minPartSize, so an upload
	// of two parts writes over 10 MiB with its assembly. Half are one part,
	// which may be of any size, to keep the bytes written down.
	size := 1 + w.rng.Int64N(w.s.cfg.maxSize)
	if w.rng.IntN(2) == 0 {
		size = minPartSize + 1 + w.rng.Int64N(256<<10)
	}
	c := w.newContent(key, size)
	data := c.bytes()
	var parts [][]byte
	for off := int64(0); off < c.size; off += minPartSize {
		parts = append(parts, data[off:min(off+minPartSize, c.size)])
	}

	bucket := &w.s.cfg.bucket
	in := &s3.CreateMultipartUploadInput{Bucket: bucket, Key: &key}
	if ct := w.setAttributes(c); ct != "" {
		in.ContentType = &ct
	}
	in.Metadata = c.metadata
	created, err := w.s.client.CreateMultipartUpload(ctx, in)
	if err != nil {
		w.fail("multipart", key, "create: %v", err)
		return
	}
	uploadID := created.UploadId
	done := false
	defer func() {
		if !done {
			_, _ = w.s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: bucket, Key: &key, UploadId: uploadID})
		}
	}()

	// Parts of plain keys are sometimes sent in aws-chunked encoding
	chunked := strings.HasPrefix(key, w.prefix+"plain/") && w.rng.IntN(3) == 0
	var completed []types.CompletedPart
	for i, p := range parts {
		n := int32(i + 1)
		// A part uploaded again replaces the one before
		if w.rng.IntN(10) == 0 {
			other := w.newContent(key, int64(len(p))).bytes()
			if _, err := w.uploadPart(ctx, key, aws.ToString(uploadID), n, other, chunked); err != nil {
				w.fail("multipart", key, "upload part %d to be replaced: %v", n, err)
				return
			}
		}
		etag, err := w.uploadPart(ctx, key, aws.ToString(uploadID), n, p, chunked)
		if err != nil {
			w.fail("multipart", key, "upload part %d: %v", n, err)
			return
		}
		completed = append(completed, types.CompletedPart{PartNumber: &n, ETag: &etag})
	}

	if w.rng.IntN(2) == 0 {
		w.checkParts(ctx, key, uploadID, parts, completed)
	}
	if w.rng.IntN(2) == 0 {
		w.checkUploads(ctx, key, aws.ToString(uploadID))
	}

	switch r := w.rng.IntN(100); {
	case r < 15:
		// Abort, after which the upload must be gone
		done = true
		if _, err := w.s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: bucket, Key: &key, UploadId: uploadID}); err != nil {
			w.fail("multipart", key, "abort: %v", err)
			return
		}
		one := int32(1)
		_, err := w.s.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: bucket, Key: &key, UploadId: uploadID, PartNumber: &one,
			Body: bytes.NewReader(parts[0]), ContentLength: aws.Int64(int64(len(parts[0]))),
		})
		if err := expectError(err, http.StatusNotFound, "NoSuchUpload"); err != nil {
			w.fail("multipart", key, "upload part after abort: %v", err)
		}
		w.checkUploadGone(ctx, key, uploadID)
		return
	case r < 35:
		// A part with the wrong ETag must be refused, leaving the upload
		wrong := slices.Clone(completed)
		wrong[0].ETag = aws.String(`"sha256-wrong"`)
		_, err := w.s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket: bucket, Key: &key, UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: wrong},
		})
		if err := expectError(err, http.StatusBadRequest, "InvalidPart"); err != nil {
			w.fail("multipart", key, "complete with wrong ETag: %v", err)
			if err == nil {
				done = true
				w.resync(ctx, key)
				return
			}
		}
	}

	out, err := w.s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: bucket, Key: &key, UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
	})
	if err != nil {
		w.fail("multipart", key, "complete: %v", err)
		w.resync(ctx, key)
		return
	}
	done = true
	w.wrote(c.size)
	c.etag = multipartETag(parts)
	if aws.ToString(out.ETag) != c.etag {
		w.fail("multipart", key, "ETag %s, want %s", aws.ToString(out.ETag), c.etag)
		c.etag = aws.ToString(out.ETag)
	}
	w.model[key] = c
	if w.rng.IntN(4) == 0 {
		w.checkUploadGone(ctx, key, uploadID)
	}
}

// uploadPart uploads a part, with the SDK or in aws-chunked encoding, and
// returns its ETag
func (w *worker) uploadPart(ctx context.Context, key, uploadID string, n int32, p []byte, chunked bool) (string, error) {
	w.wrote(int64(len(p)))
	w.s.stats.sent.Add(int64(len(p)))
	if chunked {
		mode := chunkModes[w.rng.IntN(len(chunkModes))]
		resp, err := w.s.sendChunked(ctx, w.s.httpClient, chunkedUpload{
			target:    fmt.Sprintf("/%s/%s?partNumber=%d&uploadId=%s", w.s.cfg.bucket, key, n, url.QueryEscape(uploadID)),
			mode:      mode,
			data:      p,
			chunkSize: w.chunkSize(),
		})
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("%s: status %d: %s", mode, resp.StatusCode, body)
		}
		return resp.Header.Get("ETag"), nil
	}
	in := &s3.UploadPartInput{
		Bucket: &w.s.cfg.bucket, Key: &key, UploadId: &uploadID, PartNumber: &n,
		Body: bytes.NewReader(p), ContentLength: aws.Int64(int64(len(p))),
	}
	if w.rng.IntN(2) == 0 {
		in.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	out, err := w.s.client.UploadPart(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

// checkParts lists the parts of an upload, in pages, and checks them
// against the parts uploaded
func (w *worker) checkParts(ctx context.Context, key string, uploadID *string, parts [][]byte, completed []types.CompletedPart) {
	maxParts := 1 + w.rng.Int32N(int32(len(parts))+1)
	var listed []types.Part
	var marker *string
	for range len(parts) + 2 {
		out, err := w.s.client.ListParts(ctx, &s3.ListPartsInput{
			Bucket: &w.s.cfg.bucket, Key: &key, UploadId: uploadID, MaxParts: &maxParts, PartNumberMarker: marker,
		})
		if err != nil {
			w.fail("multipart", key, "list parts: %v", err)
			return
		}
		if len(out.Parts) > int(maxParts) {
			w.fail("multipart", key, "list parts: %d parts, max-parts %d", len(out.Parts), maxParts)
		}
		listed = append(listed, out.Parts...)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		marker = out.NextPartNumberMarker
	}
	if len(listed) != len(parts) {
		w.fail("multipart", key, "list parts: %d parts, want %d", len(listed), len(parts))
		return
	}
	for i, p := range listed {
		sum := sha256.Sum256(parts[i])
		switch {
		case aws.ToInt32(p.PartNumber) != int32(i+1):
			w.fail("multipart", key, "list parts: part %d listed as part %d", i+1, aws.ToInt32(p.PartNumber))
		case aws.ToInt64(p.Size) != int64(len(parts[i])):
			w.fail("multipart", key, "list parts: part %d of %d bytes, want %d", i+1, aws.ToInt64(p.Size), len(parts[i]))
		case aws.ToString(p.ETag) != aws.ToString(completed[i].ETag):
			w.fail("multipart", key, "list parts: part %d ETag %s, want %s", i+1, aws.ToString(p.ETag), aws.ToString(completed[i].ETag))
		case aws.ToString(p.ChecksumSHA256) != base64.StdEncoding.EncodeToString(sum[:]):
			w.fail("multipart", key, "list parts: part %d checksum %s", i+1, aws.ToString(p.ChecksumSHA256))
		}
	}
}

// checkUploads lists the uploads under the worker's prefix, which a worker
// starts one at a time, so the upload in progress must be the only one
func (w *worker) checkUploads(ctx context.Context, key, uploadID string) {
	var uploads []types.MultipartUpload
	var keyMarker, idMarker *string
	for range 10 {
		out, err := w.s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket: &w.s.cfg.bucket, Prefix: &w.prefix, MaxUploads: aws.Int32(1),
			KeyMarker: keyMarker, UploadIdMarker: idMarker,
		})
		if err != nil {
			w.fail("multipart", key, "list uploads: %v", err)
			return
		}
		uploads = append(uploads, out.Uploads...)
		if !aws.ToBool(out.IsTruncated) {
			break
		}
		keyMarker, idMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	if len(uploads) != 1 || aws.ToString(uploads[0].Key) != key || aws.ToString(uploads[0].UploadId) != uploadID {
		var listed []string
		for _, u := range uploads {
			listed = append(listed, aws.ToString(u.Key)+" "+aws.ToString(u.UploadId))
		}
		w.fail("multipart", key, "list uploads: %q, want only upload %s", listed, uploadID)
	}
}

// checkUploadGone checks that an upload completed or aborted is gone
func (w *worker) checkUploadGone(ctx context.Context, key string, uploadID *string) {
	_, err := w.s.client.ListParts(ctx, &s3.ListPartsInput{Bucket: &w.s.cfg.bucket, Key: &key, UploadId: uploadID})
	if err := expectError(err, http.StatusNotFound, "NoSuchUpload"); err != nil {
		w.fail("multipart", key, "list parts of an upload that is gone: %v", err)
	}
	out, err := w.s.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &w.s.cfg.bucket, Prefix: &key})
	if err != nil {
		w.fail("multipart", key, "list uploads: %v", err)
		return
	}
	for _, u := range out.Uploads {
		if aws.ToString(u.UploadId) == aws.ToString(uploadID) {
			w.fail("multipart", key, "upload %s is listed after it is gone", aws.ToString(uploadID))
		}
	}
}

func (w *worker) presignedGet(ctx context.Context) {
	key := w.pickKey()
	c := w.model[key]
	in := &s3.GetObjectInput{Bucket: &w.s.cfg.bucket, Key: &key}
	// A presigned GET may override response headers
	override := w.rng.IntN(2) == 0
	if override {
		in.ResponseContentType = aws.String("text/x-soak")
		in.ResponseContentDisposition = aws.String(`attachment; filename="soak.bin"`)
		in.ResponseCacheControl = aws.String("no-store")
	}
	req, err := w.s.presign.PresignGetObject(ctx, in, s3.WithPresignExpires(time.Minute))
	if err != nil {
		w.fail("presigned-get", key, "presign: %v", err)
		return
	}
	hreq, _ := http.NewRequestWithContext(ctx, http.MethodGet, req.URL, nil)
	resp, err := w.s.httpClient.Do(hreq)
	if err != nil {
		w.fail("presigned-get", key, "%v", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if c == nil {
		if resp.StatusCode != http.StatusNotFound {
			w.fail("presigned-get", key, "deleted key: status %d", resp.StatusCode)
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		w.fail("presigned-get", key, "status %d", resp.StatusCode)
		return
	}
	wantType := c.contentType
	if override {
		wantType = "text/x-soak"
		if got := resp.Header.Get("Content-Disposition"); got != aws.ToString(in.ResponseContentDisposition) {
			w.fail("presigned-get", key, "Content-Disposition %q, want %q", got, aws.ToString(in.ResponseContentDisposition))
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			w.fail("presigned-get", key, "Cache-Control %q, want no-store", got)
		}
	}
	if got := resp.Header.Get("Content-Type"); got != wantType {
		w.fail("presigned-get", key, "Content-Type %q, want %q", got, wantType)
	}
	if err := c.verifyRange(resp.Body, 0, c.size); err != nil {
		w.fail("presigned-get", key, "%v", err)
	}
}

// presignedPut uploads an object with a presigned URL
func (w *worker) presignedPut(ctx context.Context) {
	key := w.pickKey()
	c := w.newContent(key, w.size())
	req, err := w.s.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: &w.s.cfg.bucket, Key: &key},
		s3.WithPresignExpires(time.Minute))
	if err != nil {
		w.fail("presigned-put", key, "presign: %v", err)
		return
	}
	data := c.bytes()
	hreq, _ := http.NewRequestWithContext(ctx, req.Method, req.URL, bytes.NewReader(data))
	hreq.ContentLength = c.size
	w.wrote(c.size)
	resp, err := w.s.httpClient.Do(hreq)
	if err != nil {
		w.fail("presigned-put", key, "%v", err)
		w.resync(ctx, key)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		w.fail("presigned-put", key, "status %d: %s", resp.StatusCode, body)
		w.resync(ctx, key)
		return
	}
	w.s.stats.sent.Add(c.size)
	c.etag = singleETag(data)
	if got := resp.Header.Get("ETag"); got != c.etag {
		w.fail("presigned-put", key, "ETag %s, want %s", got, c.etag)
	}
	w.model[key] = c
}

// rawPut sends a signed PUT of size bytes with the given payload hash and
// body, outside the SDK, which would retry or refuse to send it
func (w *worker) rawPut(ctx context.Context, key string, size int64, payloadHash string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.s.cfg.endpoint+"/"+w.s.cfg.bucket+"/"+key, body)
	if err != nil {
		return nil, err
	}
	req.ContentLength = size
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if err := w.s.signer.SignHTTP(ctx, w.s.creds, req, payloadHash, "s3", region, time.Now()); err != nil {
		return nil, err
	}
	return w.s.rawClient.Do(req)
}

// interruptedPut sends half of a PUT body and then breaks the connection.
// The key must keep its old content, and no temp file may be left.
func (w *worker) interruptedPut(ctx context.Context) {
	key := w.pickPlainKey()
	c := w.newContent(key, 2+w.size())
	data := c.bytes()
	pr, pw := io.Pipe()
	go func() {
		_, _ = pw.Write(data[:len(data)/2])
		_ = pw.CloseWithError(errors.New("soak: interrupted upload"))
	}()
	w.wrote(int64(len(data) / 2))
	resp, err := w.rawPut(ctx, key, c.size, "UNSIGNED-PAYLOAD", pr)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			w.fail("interrupted-put", key, "a PUT that sent %d of %d bytes was stored", len(data)/2, len(data))
			w.resync(ctx, key)
		}
	}
	if w.rng.IntN(2) == 0 {
		w.verifyKey(ctx, "interrupted-put", key)
	}
}

// tamperedPut sends a PUT whose body differs from the signed payload hash.
// It must be refused, and the key must keep its old content.
func (w *worker) tamperedPut(ctx context.Context) {
	key := w.pickPlainKey()
	c := w.newContent(key, 1+w.size())
	data := c.bytes()
	sum := sha256.Sum256(data)
	data[w.rng.IntN(len(data))] ^= 0x01
	w.wrote(c.size)
	resp, err := w.rawPut(ctx, key, c.size, hex.EncodeToString(sum[:]), bytes.NewReader(data))
	if err != nil {
		w.fail("tampered-put", key, "%v", err)
		w.resync(ctx, key)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), "XAmzContentSHA256Mismatch") {
		w.fail("tampered-put", key, "status %d: %s", resp.StatusCode, body)
		w.resync(ctx, key)
		return
	}
	w.verifyKey(ctx, "tampered-put", key)
}

// interruptedGet reads the start of an object and closes the response
func (w *worker) interruptedGet(ctx context.Context) {
	key := w.pickStored()
	if key == "" {
		return
	}
	c := w.model[key]
	out, err := w.s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &w.s.cfg.bucket, Key: &key})
	if err != nil {
		w.fail("interrupted-get", key, "%v", err)
		return
	}
	n := min(c.size/2, 64<<10)
	if err := c.verifyRange(io.LimitReader(out.Body, n), 0, n); err != nil {
		w.fail("interrupted-get", key, "%v", err)
	}
	_ = out.Body.Close()
}

func (w *worker) sharedKey() string {
	return fmt.Sprintf("shared/%03d", w.rng.IntN(w.s.cfg.sharedKeys))
}

// sharedPut writes a shared key, which other workers write at the same
// time, sometimes only if it does not exist
func (w *worker) sharedPut(ctx context.Context) {
	key := w.sharedKey()
	c := w.newContent(key, headerSize+w.rng.Int64N(256<<10))
	data := c.bytes()
	in := &s3.PutObjectInput{Bucket: &w.s.cfg.bucket, Key: &key, Body: bytes.NewReader(data), ContentLength: aws.Int64(c.size)}
	ifNoneMatch := w.rng.IntN(4) == 0
	if ifNoneMatch {
		in.IfNoneMatch = aws.String("*")
	}
	w.wrote(c.size)
	out, err := w.s.client.PutObject(ctx, in)
	if ifNoneMatch && httpStatus(err) == http.StatusPreconditionFailed {
		return
	}
	if err != nil {
		w.fail("shared-put", key, "%v", err)
		return
	}
	if want := singleETag(data); aws.ToString(out.ETag) != want {
		w.fail("shared-put", key, "ETag %s, want %s", aws.ToString(out.ETag), want)
	}
}

// verifyShared reads a shared key, which must hold one complete object
// written to it, with that object's ETag, or nothing
func (s *soak) verifyShared(ctx context.Context, op, key string) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.cfg.bucket, Key: &key})
	if httpStatus(err) == http.StatusNotFound {
		return
	}
	if err != nil {
		s.fail(op, key, err.Error())
		return
	}
	data, err := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if err != nil {
		s.fail(op, key, err.Error())
		return
	}
	if err := verifySelf(key, data); err != nil {
		s.fail(op, key, err.Error())
		return
	}
	if want := singleETag(data); aws.ToString(out.ETag) != want {
		s.fail(op, key, fmt.Sprintf("ETag %s does not match the body, want %s", aws.ToString(out.ETag), want))
	}
}

func (w *worker) sharedGet(ctx context.Context) { w.s.verifyShared(ctx, "shared-get", w.sharedKey()) }

func (w *worker) sharedDelete(ctx context.Context) {
	key := w.sharedKey()
	if _, err := w.s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &w.s.cfg.bucket, Key: &key}); err != nil {
		w.fail("shared-delete", key, "%v", err)
	}
}

// fill gives every owned key content, so that the store is at its full
// size before the baseline checkpoint
func (w *worker) fill() {
	for _, key := range w.keys {
		ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
		if err := w.putContent(ctx, "fill", key, w.newContent(key, w.size()), nil); err != nil {
			w.fail("fill", key, "%v", err)
		}
		cancel()
		w.payWrites(nil)
	}
}

// sweep checks every owned key and the listing of the worker's prefix
func (w *worker) sweep() {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	for _, key := range w.keys {
		w.verifyKey(ctx, "sweep", key)
	}
	found, err := w.s.listKeys(ctx, w.prefix, 1000)
	if err != nil {
		w.fail("sweep", w.prefix, "list: %v", err)
		return
	}
	want, _ := w.expectListing(w.prefix, "", "")
	w.compareListing("sweep", want, found)
}
