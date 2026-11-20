package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/espen/stupid-simple-s3/internal/auth"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

const (
	signedTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	signedTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	signedTestRegion    = "us-east-1"
	signedTestService   = "s3"
)

// streamSigning holds what a client needs to sign an aws-chunked body: the
// signing key, the request date and credential scope, and the seed
// signature of the request itself.
type streamSigning struct {
	key   []byte
	date  string // X-Amz-Date, e.g. 20261120T120000Z
	scope string // <yyyymmdd>/<region>/<service>/aws4_request
	seed  string
	time  time.Time
	// req is the signed request, to obtain a fresh production ChunkVerifier
	req *http.Request
}

func testHMAC(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func testSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// newStreamSigning signs a streaming PUT request with the AWS SDK signer
// and derives the signing key independently of the production code.
func newStreamSigning(t testing.TB, payloadMode string) *streamSigning {
	t.Helper()
	now := time.Now().UTC()
	req := httptest.NewRequest(http.MethodPut, "http://localhost:5553/bucket/key", nil)
	req.Header.Set("X-Amz-Content-Sha256", payloadMode)
	req.Header.Set("Content-Encoding", "aws-chunked")
	creds := aws.Credentials{AccessKeyID: signedTestAccessKey, SecretAccessKey: signedTestSecretKey}
	if err := v4.NewSigner().SignHTTP(context.Background(), creds, req, payloadMode, signedTestService, signedTestRegion, now); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}
	authz := req.Header.Get("Authorization")
	_, seed, ok := strings.Cut(authz, "Signature=")
	if !ok {
		t.Fatalf("no signature in %q", authz)
	}

	day := now.Format("20060102")
	key := testHMAC([]byte("AWS4"+signedTestSecretKey), day)
	key = testHMAC(key, signedTestRegion)
	key = testHMAC(key, signedTestService)
	key = testHMAC(key, "aws4_request")

	return &streamSigning{
		key:   key,
		date:  req.Header.Get("X-Amz-Date"),
		scope: day + "/" + signedTestRegion + "/" + signedTestService + "/aws4_request",
		seed:  seed,
		time:  now,
		req:   req,
	}
}

// verifier returns a fresh production ChunkVerifier for the signed request,
// built the way AuthMiddleware gets it.
func (s *streamSigning) verifier(t testing.TB) *auth.ChunkVerifier {
	t.Helper()
	var sv auth.SignatureV4
	result, err := sv.VerifyRequest(s.req, signedTestSecretKey)
	if err != nil {
		t.Fatalf("VerifyRequest: %v", err)
	}
	if result.ChunkVerifier == nil {
		t.Fatal("VerifyRequest returned no ChunkVerifier for a streaming payload")
	}
	return result.ChunkVerifier
}

// chunkSignature computes a chunk signature per the AWS SigV4 streaming spec.
func (s *streamSigning) chunkSignature(prev string, data []byte) string {
	stringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + s.date + "\n" + s.scope + "\n" + prev + "\n" +
		testSHA256Hex(nil) + "\n" + testSHA256Hex(data)
	return hex.EncodeToString(testHMAC(s.key, stringToSign))
}

// trailerSignature computes the trailer signature per the AWS SigV4 spec.
// trailerLines is each "name:value" followed by "\n".
func (s *streamSigning) trailerSignature(prev, trailerLines string) string {
	stringToSign := "AWS4-HMAC-SHA256-TRAILER\n" + s.date + "\n" + s.scope + "\n" + prev + "\n" +
		testSHA256Hex([]byte(trailerLines))
	return hex.EncodeToString(testHMAC(s.key, stringToSign))
}

// signedStream is a signed aws-chunked body, kept in parts so tests can
// tamper with individual pieces before encoding it.
type signedStream struct {
	frames   []string // each "<hex>;chunk-signature=<sig>\r\n<data>\r\n"
	final    string   // "0;chunk-signature=<sig>\r\n"
	trailers []string // "name:value" lines, without the trailer signature
	trailSig string   // x-amz-trailer-signature value, "" for none
}

func (st *signedStream) bytes() []byte {
	var b strings.Builder
	for _, f := range st.frames {
		b.WriteString(f)
	}
	b.WriteString(st.final)
	for _, tr := range st.trailers {
		b.WriteString(tr + "\r\n")
	}
	if st.trailSig != "" {
		b.WriteString("x-amz-trailer-signature:" + st.trailSig + "\r\n")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

// signChunks produces a correctly signed aws-chunked stream of chunks,
// followed by the signed trailers, if any.
func (s *streamSigning) signChunks(chunks [][]byte, trailers ...string) *signedStream {
	st := &signedStream{}
	prev := s.seed
	for _, c := range chunks {
		sig := s.chunkSignature(prev, c)
		st.frames = append(st.frames, fmt.Sprintf("%x;chunk-signature=%s\r\n%s\r\n", len(c), sig, c))
		prev = sig
	}
	sig := s.chunkSignature(prev, nil)
	st.final = "0;chunk-signature=" + sig + "\r\n"
	prev = sig
	if len(trailers) > 0 {
		var lines strings.Builder
		for _, tr := range trailers {
			lines.WriteString(tr + "\n")
		}
		st.trailers = trailers
		st.trailSig = s.trailerSignature(prev, lines.String())
	}
	return st
}

// splitChunks splits data into chunks of at most size bytes
func splitChunks(data []byte, size int) [][]byte {
	var chunks [][]byte
	for len(data) > 0 {
		n := min(size, len(data))
		chunks = append(chunks, data[:n])
		data = data[n:]
	}
	return chunks
}

func checksumTrailer(data []byte) string {
	sum := sha256.Sum256(data)
	return checksumSHA256Header + ":" + storage.EncodeChecksum(sum[:])
}

// readSignedReader reads body through an awsChunkedReader with a verifier
func readSignedReader(t testing.TB, s *streamSigning, body []byte) ([]byte, *awsChunkedReader, error) {
	t.Helper()
	r := newAWSChunkedReader(bytes.NewReader(body), 0)
	r.verifier = s.verifier(t)
	out, err := io.ReadAll(r)
	return out, r, err
}

// readSignedUpload reads body the way PutObject does: through uploadBody,
// with the verifier in the request context as set by AuthMiddleware.
func readSignedUpload(t testing.TB, s *streamSigning, body []byte, withTrailer bool) ([]byte, error) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "http://localhost:5553/bucket/key", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "aws-chunked")
	if withTrailer {
		req.Header.Set("X-Amz-Content-Sha256", auth.StreamingPayloadTrailer)
		req.Header.Set("X-Amz-Trailer", checksumSHA256Header)
	} else {
		req.Header.Set("X-Amz-Content-Sha256", auth.StreamingPayload)
	}
	req = req.WithContext(context.WithValue(req.Context(), chunkVerifierContextKey, s.verifier(t)))
	body2, err := uploadBody(req, 0)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(body2)
}

func signedTestData() []byte {
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i*7 + i/251)
	}
	return data
}

// TestSignedChunkHelperMatchesSDK checks the test's chunk signatures against
// the AWS SDK stream signer. With no event headers, its string to sign is
// the aws-chunked one: the headers hash is the hash of the empty string.
func TestSignedChunkHelperMatchesSDK(t *testing.T) {
	s := newStreamSigning(t, auth.StreamingPayload)
	seed, err := hex.DecodeString(s.seed)
	if err != nil {
		t.Fatal(err)
	}
	creds := aws.Credentials{AccessKeyID: signedTestAccessKey, SecretAccessKey: signedTestSecretKey}
	sdk := v4.NewStreamSigner(creds, signedTestService, signedTestRegion, seed)

	prev := s.seed
	for _, c := range append(splitChunks(signedTestData(), 4096), nil) {
		want, err := sdk.GetSignature(context.Background(), nil, c, s.time)
		if err != nil {
			t.Fatal(err)
		}
		got := s.chunkSignature(prev, c)
		if got != hex.EncodeToString(want) {
			t.Fatalf("chunk of %d bytes: signature %s, SDK %x", len(c), got, want)
		}
		prev = got
	}
}

func TestSignedAWSChunkedReaderValid(t *testing.T) {
	s := newStreamSigning(t, auth.StreamingPayload)
	data := signedTestData()
	for _, size := range []int{1, 100, 4096, len(data)} {
		body := s.signChunks(splitChunks(data, size)).bytes()
		got, _, err := readSignedReader(t, s, body)
		if err != nil {
			t.Fatalf("chunk size %d: %v", size, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("chunk size %d: data differs", size)
		}
		got, err = readSignedUpload(t, s, body, false)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("chunk size %d: uploadBody: err=%v, equal=%v", size, err, bytes.Equal(got, data))
		}
	}

	// An empty body is just the signed final chunk
	got, _, err := readSignedReader(t, s, s.signChunks(nil).bytes())
	if err != nil || len(got) != 0 {
		t.Fatalf("empty body: got %d bytes, err=%v", len(got), err)
	}
}

func TestSignedAWSChunkedReaderValidTrailer(t *testing.T) {
	s := newStreamSigning(t, auth.StreamingPayloadTrailer)
	data := signedTestData()
	trailer := checksumTrailer(data)
	body := s.signChunks(splitChunks(data, 4096), trailer).bytes()

	got, r, err := readSignedReader(t, s, body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data differs")
	}
	if _, want, _ := strings.Cut(trailer, ":"); r.Trailer(checksumSHA256Header) != want {
		t.Fatalf("trailer = %q, want %q", r.Trailer(checksumSHA256Header), want)
	}

	got, err = readSignedUpload(t, s, body, true)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("uploadBody: err=%v, equal=%v", err, bytes.Equal(got, data))
	}
}

func TestSignedAWSChunkedReaderTampered(t *testing.T) {
	data := signedTestData()
	flipHex := func(sig string) string {
		b := []byte(sig)
		if b[10] == '0' {
			b[10] = '1'
		} else {
			b[10] = '0'
		}
		return string(b)
	}

	tests := []struct {
		name    string
		trailer bool
		tamper  func(s *streamSigning, st *signedStream)
	}{
		{"chunk data", false, func(_ *streamSigning, st *signedStream) {
			f := []byte(st.frames[1])
			f[len(f)-3] ^= 1
			st.frames[1] = string(f)
		}},
		{"middle chunk signature", false, func(_ *streamSigning, st *signedStream) {
			hdr, rest, _ := strings.Cut(st.frames[1], "\r\n")
			size, sig, _ := strings.Cut(hdr, ";chunk-signature=")
			st.frames[1] = size + ";chunk-signature=" + flipHex(sig) + "\r\n" + rest
		}},
		{"final chunk signature", false, func(_ *streamSigning, st *signedStream) {
			sig := strings.TrimSuffix(strings.TrimPrefix(st.final, "0;chunk-signature="), "\r\n")
			st.final = "0;chunk-signature=" + flipHex(sig) + "\r\n"
		}},
		{"final chunk signature missing", false, func(_ *streamSigning, st *signedStream) {
			st.final = "0\r\n"
		}},
		{"final chunk signature empty", false, func(_ *streamSigning, st *signedStream) {
			st.final = "0;chunk-signature=\r\n"
		}},
		{"middle chunk signature missing", false, func(_ *streamSigning, st *signedStream) {
			_, rest, _ := strings.Cut(st.frames[1], ";chunk-signature=")
			_, rest, _ = strings.Cut(rest, "\r\n")
			st.frames[1] = fmt.Sprintf("%x\r\n%s", 4096, rest)
		}},
		{"signature uppercased", false, func(_ *streamSigning, st *signedStream) {
			st.frames[0] = strings.ToUpper(st.frames[0][:strings.Index(st.frames[0], "\r\n")]) + st.frames[0][strings.Index(st.frames[0], "\r\n"):]
		}},
		{"chunks reordered", false, func(_ *streamSigning, st *signedStream) {
			st.frames[0], st.frames[1] = st.frames[1], st.frames[0]
		}},
		{"middle chunk dropped", false, func(_ *streamSigning, st *signedStream) {
			st.frames = append(st.frames[:1], st.frames[2:]...)
		}},
		{"last data chunk dropped", false, func(_ *streamSigning, st *signedStream) {
			st.frames = st.frames[:len(st.frames)-1]
		}},
		{"chunk replayed", false, func(_ *streamSigning, st *signedStream) {
			st.frames = append(st.frames[:2], st.frames[1:]...)
		}},
		{"signed with another seed", false, func(s *streamSigning, st *signedStream) {
			other := *s
			other.seed = flipHex(s.seed)
			*st = *other.signChunks(splitChunks(data, 4096))
		}},
		{"trailer value", true, func(_ *streamSigning, st *signedStream) {
			// Another well-formed checksum, so only the signature can catch it
			st.trailers = []string{checksumTrailer([]byte("other data"))}
		}},
		{"trailer name", true, func(_ *streamSigning, st *signedStream) {
			st.trailers = []string{strings.Replace(st.trailers[0], "sha256", "sha257", 1)}
		}},
		{"trailer added", true, func(_ *streamSigning, st *signedStream) {
			st.trailers = append(st.trailers, "x-amz-meta-evil:1")
		}},
		{"trailer signature", true, func(_ *streamSigning, st *signedStream) {
			st.trailSig = flipHex(st.trailSig)
		}},
		{"trailer signature missing", true, func(_ *streamSigning, st *signedStream) {
			st.trailSig = ""
		}},
		{"trailer signature is the final chunk signature", true, func(_ *streamSigning, st *signedStream) {
			st.trailSig = strings.TrimSuffix(strings.TrimPrefix(st.final, "0;chunk-signature="), "\r\n")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode := auth.StreamingPayload
			var trailers []string
			if tt.trailer {
				mode = auth.StreamingPayloadTrailer
				trailers = []string{checksumTrailer(data)}
			}
			s := newStreamSigning(t, mode)
			st := s.signChunks(splitChunks(data, 4096), trailers...)
			tt.tamper(s, st)
			body := st.bytes()

			got, _, err := readSignedReader(t, s, body)
			if err == nil {
				t.Fatalf("tampered stream read without error (data equal: %v)", bytes.Equal(got, data))
			}
			if !errors.Is(err, auth.ErrChunkSignatureMismatch) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Logf("error: %v", err)
			}
			if _, err := readSignedUpload(t, s, body, tt.trailer); err == nil {
				t.Fatal("tampered stream read through uploadBody without error")
			}
		})
	}
}

// TestSignedAWSChunkedReaderSignatureErrors checks that signature failures
// are reported as ErrChunkSignatureMismatch rather than a generic error.
func TestSignedAWSChunkedReaderSignatureErrors(t *testing.T) {
	data := signedTestData()
	s := newStreamSigning(t, auth.StreamingPayloadTrailer)

	st := s.signChunks(splitChunks(data, 4096), checksumTrailer(data))
	st.final = "0;chunk-signature=" + strings.Repeat("0", 64) + "\r\n"
	if _, _, err := readSignedReader(t, s, st.bytes()); !errors.Is(err, auth.ErrChunkSignatureMismatch) {
		t.Errorf("bad final chunk signature: err = %v, want ErrChunkSignatureMismatch", err)
	}

	st = s.signChunks(splitChunks(data, 4096), checksumTrailer(data))
	st.trailers = []string{checksumTrailer([]byte("x"))}
	if _, _, err := readSignedReader(t, s, st.bytes()); !errors.Is(err, auth.ErrChunkSignatureMismatch) {
		t.Errorf("bad trailer value: err = %v, want ErrChunkSignatureMismatch", err)
	}

	st = s.signChunks(splitChunks(data, 4096), checksumTrailer(data))
	st.trailSig = ""
	if _, _, err := readSignedReader(t, s, st.bytes()); !errors.Is(err, auth.ErrChunkSignatureMismatch) {
		t.Errorf("missing trailer signature: err = %v, want ErrChunkSignatureMismatch", err)
	}
}

// TestSignedAWSChunkedReaderTrailerStripped checks that removing the signed
// trailers entirely is caught. The reader only verifies a trailer signature
// when trailers were sent, so this relies on uploadBody requiring the
// checksum trailer named in X-Amz-Trailer.
func TestSignedAWSChunkedReaderTrailerStripped(t *testing.T) {
	data := signedTestData()
	s := newStreamSigning(t, auth.StreamingPayloadTrailer)
	st := s.signChunks(splitChunks(data, 4096), checksumTrailer(data))
	st.trailers = nil
	st.trailSig = ""
	if _, err := readSignedUpload(t, s, st.bytes(), true); err == nil {
		t.Fatal("stream with its trailers stripped read without error")
	}
}

// FuzzSignedAWSChunkedReader signs data as an aws-chunked stream, checks it
// reads back intact, then flips one bit of the stream. Reading the altered
// stream must fail, or return exactly the original data: a few bytes are
// legitimately not covered by any signature (the CRLF after chunk data, the
// case of hex chunk sizes, the blank line ending the stream), but nothing
// may change the data that is read.
func FuzzSignedAWSChunkedReader(f *testing.F) {
	f.Add([]byte("hello world"), uint16(4), uint32(0))
	f.Add([]byte(""), uint16(0), uint32(7))
	f.Add([]byte("a"), uint16(1), uint32(100))
	f.Add(bytes.Repeat([]byte("0123456789abcdef"), 64), uint16(256), uint32(12345))
	f.Add(bytes.Repeat([]byte{0xff}, 33), uint16(10), uint32(999999))

	// VerifyRequest checks the signing time against the clock on every
	// iteration, and a fuzz worker can run for longer than
	// auth.MaxClockSkew, so sign again well before the signature gets that
	// old
	var mu sync.Mutex
	plain := newStreamSigning(f, auth.StreamingPayload)
	withTrailer := newStreamSigning(f, auth.StreamingPayloadTrailer)
	signing := func(t *testing.T, trailer bool) *streamSigning {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(plain.time) > auth.MaxClockSkew/3 {
			plain = newStreamSigning(t, auth.StreamingPayload)
			withTrailer = newStreamSigning(t, auth.StreamingPayloadTrailer)
		}
		if trailer {
			return withTrailer
		}
		return plain
	}

	f.Fuzz(func(t *testing.T, data []byte, chunkSize uint16, flip uint32) {
		size := max(1, int(chunkSize))
		trailer := len(data)%2 == 1
		s := signing(t, trailer)
		var trailers []string
		if trailer {
			trailers = []string{checksumTrailer(data)}
		}
		stream := s.signChunks(splitChunks(data, size), trailers...).bytes()

		got, err := readSignedUpload(t, s, stream, trailer)
		if err != nil {
			t.Fatalf("valid stream: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("valid stream: data differs")
		}

		idx := int(flip % uint32(len(stream)))
		bit := byte(1) << ((flip / uint32(len(stream))) % 8)
		altered := bytes.Clone(stream)
		altered[idx] ^= bit

		got, err = readSignedUpload(t, s, altered, trailer)
		if err == nil && !bytes.Equal(got, data) {
			t.Fatalf("bit flip at %d (%q -> %q) accepted with different data", idx, stream[idx], altered[idx])
		}
	})
}
