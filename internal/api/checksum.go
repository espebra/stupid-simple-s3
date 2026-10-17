package api

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"net/http"
	"strings"

	"github.com/espen/stupid-simple-s3/internal/auth"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

const checksumSHA256Header = "x-amz-checksum-sha256"

// errInvalidChecksumHeader is returned when the x-amz-checksum-sha256 header is not a base64 SHA-256
var errInvalidChecksumHeader = errors.New("invalid x-amz-checksum-sha256 header")

// sha256Verifier calculates the SHA-256 of the data read through it and, at
// the end of the stream, compares it with the checksum sent by the client.
// A mismatch is reported as storage.ErrBadDigest instead of io.EOF, so the
// storage layer discards the upload before it is committed.
type sha256Verifier struct {
	r        io.Reader
	hash     hash.Hash
	expected func() string
}

func (v *sha256Verifier) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.hash.Write(p[:n])
	if err == io.EOF && storage.EncodeChecksum(v.hash.Sum(nil)) != v.expected() {
		return n, storage.ErrBadDigest
	}
	return n, err
}

// uploadBody returns the request body for PutObject and UploadPart, decoding
// AWS chunked encoding and verifying the client's SHA-256 checksum when one is
// sent, either as a header or as an aws-chunked trailer.
// Checksums using other algorithms are not verified.
func uploadBody(r *http.Request, maxChunkSize int64) (io.Reader, error) {
	var body io.Reader = r.Body
	var chunked *awsChunkedReader
	if isAWSChunkedEncoding(r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256")) {
		chunked = newAWSChunkedReader(r.Body, maxChunkSize)
		// Set by AuthMiddleware for signed aws-chunked uploads
		chunked.verifier, _ = r.Context().Value(chunkVerifierContextKey).(*auth.ChunkVerifier)
		body = chunked
	}

	if expected := r.Header.Get(checksumSHA256Header); expected != "" {
		if decoded, err := base64.StdEncoding.DecodeString(expected); err != nil || len(decoded) != sha256.Size {
			return nil, errInvalidChecksumHeader
		}
		return &sha256Verifier{r: body, hash: sha256.New(), expected: func() string { return expected }}, nil
	}

	if chunked != nil && hasTrailer(r.Header.Get("X-Amz-Trailer"), checksumSHA256Header) {
		return &sha256Verifier{r: body, hash: sha256.New(), expected: func() string { return chunked.Trailer(checksumSHA256Header) }}, nil
	}

	return body, nil
}

// hasTrailer reports whether the comma-separated x-amz-trailer header value names the given trailer
func hasTrailer(header, name string) bool {
	for _, t := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(t), name) {
			return true
		}
	}
	return false
}

// setChecksumHeaders adds the stored SHA-256 checksum to a response for the full object.
// The headers are sent regardless of x-amz-checksum-mode so cached responses are
// the same for all clients. Objects stored before SHA-256 checksums were introduced
// have none. Multipart objects have a composite checksum with a "-N" suffix, which
// cannot occur in a plain base64 value.
func setChecksumHeaders(w http.ResponseWriter, checksumSHA256 string) {
	if checksumSHA256 == "" {
		return
	}
	checksumType := "FULL_OBJECT"
	if strings.Contains(checksumSHA256, "-") {
		checksumType = "COMPOSITE"
	}
	w.Header().Set(checksumSHA256Header, checksumSHA256)
	w.Header().Set("x-amz-checksum-type", checksumType)
}
