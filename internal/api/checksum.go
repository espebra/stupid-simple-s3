package api

import (
	"encoding/base64"
	"errors"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strings"

	"github.com/espen/stupid-simple-s3/internal/storage"
)

const checksumCRC32Header = "x-amz-checksum-crc32"

// errInvalidChecksumHeader is returned when the x-amz-checksum-crc32 header is not a base64 CRC32
var errInvalidChecksumHeader = errors.New("invalid x-amz-checksum-crc32 header")

// crc32Verifier calculates the CRC32 of the data read through it and, at the
// end of the stream, compares it with the checksum sent by the client.
// A mismatch is reported as storage.ErrBadDigest instead of io.EOF, so the
// storage layer discards the upload before it is committed.
type crc32Verifier struct {
	r        io.Reader
	hash     hash.Hash32
	expected func() string
}

func (v *crc32Verifier) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	v.hash.Write(p[:n])
	if err == io.EOF && storage.EncodeCRC32(v.hash.Sum32()) != v.expected() {
		return n, storage.ErrBadDigest
	}
	return n, err
}

// uploadBody returns the request body for PutObject and UploadPart, decoding
// AWS chunked encoding and verifying the client's CRC32 checksum when one is
// sent, either as a header or as an aws-chunked trailer.
// Checksums using other algorithms are not verified.
func uploadBody(r *http.Request, maxChunkSize int64) (io.Reader, error) {
	var body io.Reader = r.Body
	var chunked *awsChunkedReader
	if isAWSChunkedEncoding(r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256")) {
		chunked = newAWSChunkedReader(r.Body, maxChunkSize)
		body = chunked
	}

	if expected := r.Header.Get(checksumCRC32Header); expected != "" {
		if decoded, err := base64.StdEncoding.DecodeString(expected); err != nil || len(decoded) != 4 {
			return nil, errInvalidChecksumHeader
		}
		return &crc32Verifier{r: body, hash: crc32.NewIEEE(), expected: func() string { return expected }}, nil
	}

	if chunked != nil && hasTrailer(r.Header.Get("X-Amz-Trailer"), checksumCRC32Header) {
		return &crc32Verifier{r: body, hash: crc32.NewIEEE(), expected: func() string { return chunked.Trailer(checksumCRC32Header) }}, nil
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

// setChecksumHeaders adds the stored CRC32 checksum to a response for the full object.
// The headers are sent regardless of x-amz-checksum-mode so cached responses are
// the same for all clients. Objects stored before checksums were introduced have none.
func setChecksumHeaders(w http.ResponseWriter, checksumCRC32 string) {
	if checksumCRC32 == "" {
		return
	}
	w.Header().Set(checksumCRC32Header, checksumCRC32)
	w.Header().Set("x-amz-checksum-type", "FULL_OBJECT")
}
