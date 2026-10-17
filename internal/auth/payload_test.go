package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestChunkVerifierAWSExample checks chunk signatures against the example in
// the AWS documentation for signed aws-chunked uploads ("Signature
// Calculations for the Authorization Header: Transferring Payload in Multiple
// Chunks"): a 66,560-byte object of 'a's sent as a 65,536-byte chunk, a
// 1,024-byte chunk and the final empty chunk.
func TestChunkVerifierAWSExample(t *testing.T) {
	s := &SignatureV4{}
	newVerifier := func() *ChunkVerifier {
		return &ChunkVerifier{
			signingKey:    s.deriveSigningKey("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "20130524", "us-east-1", "s3"),
			amzDate:       "20130524T000000Z",
			scope:         "20130524/us-east-1/s3/aws4_request",
			prevSignature: "4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9",
		}
	}
	chunks := []struct {
		size      int
		signature string
	}{
		{65536, "ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648"},
		{1024, "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497"},
		{0, "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"},
	}

	v := newVerifier()
	for i, c := range chunks {
		sum := sha256.Sum256([]byte(strings.Repeat("a", c.size)))
		if err := v.VerifyChunk(sum[:], c.signature); err != nil {
			t.Fatalf("chunk %d: %v", i, err)
		}
	}

	t.Run("modified chunk data", func(t *testing.T) {
		v := newVerifier()
		sum := sha256.Sum256([]byte(strings.Repeat("a", 65535) + "b"))
		if err := v.VerifyChunk(sum[:], chunks[0].signature); !errors.Is(err, ErrChunkSignatureMismatch) {
			t.Errorf("err = %v, want ErrChunkSignatureMismatch", err)
		}
	})

	t.Run("chunks out of order", func(t *testing.T) {
		v := newVerifier()
		sum := sha256.Sum256([]byte(strings.Repeat("a", 1024)))
		if err := v.VerifyChunk(sum[:], chunks[1].signature); !errors.Is(err, ErrChunkSignatureMismatch) {
			t.Errorf("err = %v, want ErrChunkSignatureMismatch", err)
		}
	})

	t.Run("missing signature", func(t *testing.T) {
		v := newVerifier()
		sum := sha256.Sum256([]byte(strings.Repeat("a", 65536)))
		if err := v.VerifyChunk(sum[:], ""); !errors.Is(err, ErrChunkSignatureMismatch) {
			t.Errorf("err = %v, want ErrChunkSignatureMismatch", err)
		}
	})
}

// TestChunkVerifierTrailer checks the trailer signature, computed as AWS
// documents it: the HMAC of "AWS4-HMAC-SHA256-TRAILER", the date, the scope,
// the previous signature and the SHA-256 of the trailer lines.
func TestChunkVerifierTrailer(t *testing.T) {
	s := &SignatureV4{}
	key := s.deriveSigningKey("secret", "20261017", "us-east-1", "s3")
	trailers := "x-amz-checksum-sha256:n4bQgYhMfWWaL+qgxVrQFaO/TxsrC4Is0V1sFbDwCgg=\n"
	trailerHash := sha256.Sum256([]byte(trailers))
	stringToSign := "AWS4-HMAC-SHA256-TRAILER\n20261017T120000Z\n20261017/us-east-1/s3/aws4_request\nprevsig\n" + hex.EncodeToString(trailerHash[:])
	signature := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))

	newVerifier := func() *ChunkVerifier {
		return &ChunkVerifier{signingKey: key, amzDate: "20261017T120000Z", scope: "20261017/us-east-1/s3/aws4_request", prevSignature: "prevsig"}
	}
	if err := newVerifier().VerifyTrailer(trailers, signature); err != nil {
		t.Errorf("VerifyTrailer: %v", err)
	}
	if err := newVerifier().VerifyTrailer(strings.Replace(trailers, "n4bQ", "AAAA", 1), signature); !errors.Is(err, ErrChunkSignatureMismatch) {
		t.Errorf("modified trailer: err = %v, want ErrChunkSignatureMismatch", err)
	}
}

func TestPayloadVerifier(t *testing.T) {
	body := "request body"
	sum := sha256.Sum256([]byte(body))

	t.Run("matching body", func(t *testing.T) {
		v := NewPayloadVerifier(io.NopCloser(strings.NewReader(body)), hex.EncodeToString(sum[:]))
		got, err := io.ReadAll(v)
		if err != nil || string(got) != body {
			t.Errorf("ReadAll = %q, %v", got, err)
		}
	})

	t.Run("uppercase hash", func(t *testing.T) {
		v := NewPayloadVerifier(io.NopCloser(strings.NewReader(body)), strings.ToUpper(hex.EncodeToString(sum[:])))
		if _, err := io.ReadAll(v); err != nil {
			t.Errorf("ReadAll: %v", err)
		}
	})

	t.Run("modified body", func(t *testing.T) {
		v := NewPayloadVerifier(io.NopCloser(strings.NewReader("tampered body")), hex.EncodeToString(sum[:]))
		if _, err := io.ReadAll(v); !errors.Is(err, ErrContentSHA256Mismatch) {
			t.Errorf("err = %v, want ErrContentSHA256Mismatch", err)
		}
	})
}

func TestIsPayloadHash(t *testing.T) {
	for value, want := range map[string]bool{
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855": true,
		"UNSIGNED-PAYLOAD":                   false,
		StreamingPayload:                     false,
		"STREAMING-UNSIGNED-PAYLOAD-TRAILER": false,
		"":                                   false,
		strings.Repeat("g", 64):              false,
	} {
		if got := IsPayloadHash(value); got != want {
			t.Errorf("IsPayloadHash(%q) = %v, want %v", value, got, want)
		}
	}
}
