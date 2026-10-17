package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strings"
)

const (
	// StreamingPayload is the X-Amz-Content-Sha256 value of an aws-chunked
	// upload whose chunks are each signed
	StreamingPayload = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	// StreamingPayloadTrailer is StreamingPayload with signed trailers
	StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	chunkAlgorithm   = "AWS4-HMAC-SHA256-PAYLOAD"
	trailerAlgorithm = "AWS4-HMAC-SHA256-TRAILER"
)

// ErrContentSHA256Mismatch is returned at the end of a request body whose
// SHA-256 does not match the signed X-Amz-Content-Sha256 header
var ErrContentSHA256Mismatch = errors.New("x-amz-content-sha256 does not match the request body")

// ErrChunkSignatureMismatch is returned when a chunk or trailer of a signed
// aws-chunked upload is missing its signature or the signature is wrong
var ErrChunkSignatureMismatch = errors.New("aws-chunked signature mismatch")

// emptySHA256 is the hex SHA-256 of an empty string
var emptySHA256 = hex.EncodeToString(sha256.New().Sum(nil))

// IsPayloadHash reports whether an X-Amz-Content-Sha256 value is the hex
// SHA-256 of the body, rather than a marker such as UNSIGNED-PAYLOAD
func IsPayloadHash(value string) bool {
	if len(value) != 2*sha256.Size {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// payloadVerifier hashes a request body as it is read and, at the end of
// the body, reports ErrContentSHA256Mismatch instead of io.EOF if the hash
// differs from the signed one.
type payloadVerifier struct {
	io.ReadCloser
	hash     hash.Hash
	expected string
}

// NewPayloadVerifier wraps body so that reading it to the end fails with
// ErrContentSHA256Mismatch unless its SHA-256 is expectedHex. The signature
// covers the X-Amz-Content-Sha256 header, not the body, so without this a
// body changed in transit would be accepted.
func NewPayloadVerifier(body io.ReadCloser, expectedHex string) io.ReadCloser {
	return &payloadVerifier{ReadCloser: body, hash: sha256.New(), expected: strings.ToLower(expectedHex)}
}

func (v *payloadVerifier) Read(p []byte) (int, error) {
	n, err := v.ReadCloser.Read(p)
	v.hash.Write(p[:n])
	if err == io.EOF && hex.EncodeToString(v.hash.Sum(nil)) != v.expected {
		return n, ErrContentSHA256Mismatch
	}
	return n, err
}

// ChunkVerifier verifies the chunk and trailer signatures of a signed
// aws-chunked upload. Each signature chains from the previous one, starting
// from the signature of the request itself.
type ChunkVerifier struct {
	signingKey    []byte
	amzDate       string
	scope         string
	prevSignature string
}

// VerifyChunk checks the signature of the next chunk, given the SHA-256 of
// its data. The final, empty chunk is signed too.
func (c *ChunkVerifier) VerifyChunk(dataHash []byte, signature string) error {
	stringToSign := strings.Join([]string{
		chunkAlgorithm,
		c.amzDate,
		c.scope,
		c.prevSignature,
		emptySHA256,
		hex.EncodeToString(dataHash),
	}, "\n")
	return c.verify(stringToSign, signature)
}

// VerifyTrailer checks the signature of the trailers that follow the final
// chunk. trailers holds the trailer lines other than x-amz-trailer-signature,
// each as "name:value\n" in the order they were sent.
func (c *ChunkVerifier) VerifyTrailer(trailers, signature string) error {
	trailerHash := sha256.Sum256([]byte(trailers))
	stringToSign := strings.Join([]string{
		trailerAlgorithm,
		c.amzDate,
		c.scope,
		c.prevSignature,
		hex.EncodeToString(trailerHash[:]),
	}, "\n")
	return c.verify(stringToSign, signature)
}

func (c *ChunkVerifier) verify(stringToSign, signature string) error {
	expected := hex.EncodeToString(hmacSHA256(c.signingKey, []byte(stringToSign)))
	if signature == "" || !hmac.Equal([]byte(expected), []byte(signature)) {
		return ErrChunkSignatureMismatch
	}
	c.prevSignature = signature
	return nil
}
