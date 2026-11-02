package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The X-Amz-Content-Sha256 values of aws-chunked uploads. The SDK only
// sends these over HTTPS, so soak encodes the uploads itself.
const (
	streamingSigned          = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingSignedTrailer   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	streamingUnsignedTrailer = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

var chunkModes = []string{streamingSigned, streamingSignedTrailer, streamingUnsignedTrailer}

var emptySHA256 = hex.EncodeToString(sha256.New().Sum(nil))

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

// chunkSigner signs the chunks and trailer of an aws-chunked upload. Each
// signature chains from the previous one, starting from the request's.
type chunkSigner struct {
	key   []byte
	date  string
	scope string
	prev  string
}

func newChunkSigner(secret, amzDate, seedSignature string) *chunkSigner {
	day := amzDate[:8]
	key := hmacSHA256([]byte("AWS4"+secret), day)
	key = hmacSHA256(key, region)
	key = hmacSHA256(key, "s3")
	key = hmacSHA256(key, "aws4_request")
	return &chunkSigner{key: key, date: amzDate, scope: day + "/" + region + "/s3/aws4_request", prev: seedSignature}
}

func (cs *chunkSigner) sign(parts ...string) string {
	cs.prev = hex.EncodeToString(hmacSHA256(cs.key, strings.Join(parts, "\n")))
	return cs.prev
}

func (cs *chunkSigner) chunk(data []byte) string {
	sum := sha256.Sum256(data)
	return cs.sign("AWS4-HMAC-SHA256-PAYLOAD", cs.date, cs.scope, cs.prev, emptySHA256, hex.EncodeToString(sum[:]))
}

func (cs *chunkSigner) trailer(trailers string) string {
	sum := sha256.Sum256([]byte(trailers))
	return cs.sign("AWS4-HMAC-SHA256-TRAILER", cs.date, cs.scope, cs.prev, hex.EncodeToString(sum[:]))
}

// encodeChunked encodes data in aws-chunked encoding, in chunks of
// chunkSize and a final empty chunk, followed by an x-amz-checksum-sha256
// trailer if checksum is not "". With a signer, the chunks and the trailer
// are signed.
func encodeChunked(data []byte, chunkSize int, checksum string, cs *chunkSigner) []byte {
	var b bytes.Buffer
	b.Grow(len(data) + (len(data)/chunkSize+2)*100)
	header := func(p []byte) {
		b.WriteString(strconv.FormatInt(int64(len(p)), 16))
		if cs != nil {
			b.WriteString(";chunk-signature=" + cs.chunk(p))
		}
		b.WriteString("\r\n")
	}
	for off := 0; off < len(data); off += chunkSize {
		p := data[off:min(off+chunkSize, len(data))]
		header(p)
		b.Write(p)
		b.WriteString("\r\n")
	}
	header(nil)
	if checksum != "" {
		line := "x-amz-checksum-sha256:" + checksum
		b.WriteString(line + "\r\n")
		if cs != nil {
			b.WriteString("x-amz-trailer-signature:" + cs.trailer(line+"\n") + "\r\n")
		}
	}
	b.WriteString("\r\n")
	return b.Bytes()
}

// chunkedUpload is a PUT of data in aws-chunked encoding
type chunkedUpload struct {
	// target is the escaped path and query of the request
	target    string
	mode      string
	data      []byte
	chunkSize int
	header    http.Header
	// tamper changes the encoded body after it is signed
	tamper func(body []byte)
	// interrupt sends half of the body and then breaks the connection
	interrupt bool
}

// sendChunked signs and sends u. The response is the caller's to close.
func (s *soak) sendChunked(ctx context.Context, client *http.Client, u chunkedUpload) (*http.Response, error) {
	checksum := ""
	if u.mode != streamingSigned {
		sum := sha256.Sum256(u.data)
		checksum = base64.StdEncoding.EncodeToString(sum[:])
	}
	signed := u.mode != streamingUnsignedTrailer

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.cfg.endpoint+u.target, nil)
	if err != nil {
		return nil, err
	}
	for name, values := range u.header {
		req.Header[name] = values
	}
	req.Header.Set("X-Amz-Content-Sha256", u.mode)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(u.data)))
	if checksum != "" {
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-sha256")
	}
	// The signatures have a fixed length, so a body encoded with any
	// signer gives the Content-Length to sign the request with
	var lengthSigner *chunkSigner
	if signed {
		lengthSigner = &chunkSigner{}
	}
	req.ContentLength = int64(len(encodeChunked(u.data, u.chunkSize, checksum, lengthSigner)))
	if err := s.signer.SignHTTP(ctx, s.creds, req, u.mode, "s3", region, time.Now()); err != nil {
		return nil, err
	}

	var cs *chunkSigner
	if signed {
		_, seed, ok := strings.Cut(req.Header.Get("Authorization"), "Signature=")
		if !ok {
			return nil, errors.New("no signature in the signed request")
		}
		cs = newChunkSigner(s.cfg.secretKey, req.Header.Get("X-Amz-Date"), seed)
	}
	body := encodeChunked(u.data, u.chunkSize, checksum, cs)
	if int64(len(body)) != req.ContentLength {
		return nil, fmt.Errorf("encoded body is %d bytes, signed as %d", len(body), req.ContentLength)
	}
	if u.tamper != nil {
		u.tamper(body)
	}
	if u.interrupt {
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(body[:len(body)/2])
			_ = pw.CloseWithError(errors.New("soak: interrupted upload"))
		}()
		req.Body = pr
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return client.Do(req)
}

// chunkSize returns the size of the chunks to send a body in
func (w *worker) chunkSize() int {
	return 1<<10 + w.rng.IntN(127<<10)
}

// chunkedPut uploads an object in aws-chunked encoding, signed or with an
// unsigned trailer. Some uploads have a chunk changed after signing, or
// are broken off halfway: those must be refused, and leave the key as it
// was, with no temp file.
func (w *worker) chunkedPut(ctx context.Context) {
	key := w.pickPlainKey()
	mode := chunkModes[w.rng.IntN(len(chunkModes))]
	c := w.newContent(key, 1+w.size())
	data := c.bytes()
	u := chunkedUpload{
		target:    "/" + w.s.cfg.bucket + "/" + key,
		mode:      mode,
		data:      data,
		chunkSize: w.chunkSize(),
		header:    http.Header{},
	}
	w.wrote(c.size)

	switch w.rng.IntN(10) {
	case 0:
		// Flip the first data byte: the chunk signature, or for an unsigned
		// upload the trailing checksum, no longer matches
		u.tamper = func(body []byte) { body[bytes.Index(body, []byte("\r\n"))+2] ^= 0x01 }
		resp, err := w.s.sendChunked(ctx, w.s.rawClient, u)
		if err != nil {
			// The server may refuse the upload at the tampered chunk and
			// close the connection before the rest of the body is sent
			w.verifyKey(ctx, "chunked-put", key)
			return
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		wantStatus, wantCode := http.StatusForbidden, "SignatureDoesNotMatch"
		if mode == streamingUnsignedTrailer {
			wantStatus, wantCode = http.StatusBadRequest, "BadDigest"
		}
		if resp.StatusCode != wantStatus || !strings.Contains(string(body), wantCode) {
			w.fail("chunked-put", key, "tampered %s: status %d, want %d %s: %s", mode, resp.StatusCode, wantStatus, wantCode, body)
			w.resync(ctx, key)
			return
		}
		w.verifyKey(ctx, "chunked-put", key)
		return
	case 1:
		u.interrupt = true
		resp, err := w.s.sendChunked(ctx, w.s.rawClient, u)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				w.fail("chunked-put", key, "a %s upload broken off halfway was stored", mode)
				w.resync(ctx, key)
			}
		}
		if w.rng.IntN(2) == 0 {
			w.verifyKey(ctx, "chunked-put", key)
		}
		return
	}

	if ct := w.setAttributes(c); ct != "" {
		u.header.Set("Content-Type", ct)
	}
	for k, v := range c.metadata {
		u.header.Set("X-Amz-Meta-"+k, v)
	}
	resp, err := w.s.sendChunked(ctx, w.s.httpClient, u)
	if err != nil {
		w.fail("chunked-put", key, "%s: %v", mode, err)
		w.resync(ctx, key)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		w.fail("chunked-put", key, "%s: status %d: %s", mode, resp.StatusCode, body)
		w.resync(ctx, key)
		return
	}
	w.s.stats.sent.Add(c.size)
	c.etag = singleETag(data)
	if got := resp.Header.Get("ETag"); got != c.etag {
		w.fail("chunked-put", key, "%s: ETag %s, want %s", mode, got, c.etag)
	}
	if got, want := resp.Header.Get("X-Amz-Checksum-Sha256"), etagChecksum(c.etag); got != want {
		w.fail("chunked-put", key, "%s: x-amz-checksum-sha256 %s, want %s", mode, got, want)
	}
	w.model[key] = c
	if w.rng.IntN(2) == 0 {
		w.verifyKey(ctx, "chunked-put", key)
	}
}
