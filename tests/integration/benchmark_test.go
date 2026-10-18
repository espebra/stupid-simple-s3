package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// signedRequest returns a request signed by the AWS SDK signer, with the
// payload hash of body, for TestAccessKeyID
func signedRequest(b *testing.B, method, target string, body []byte) *http.Request {
	b.Helper()
	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	// A request received by the server has its Content-Length in the
	// header map, and the signer signs it
	if len(body) > 0 {
		req.Header.Set("Content-Length", fmt.Sprint(len(body)))
	}
	sum := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	creds := aws.Credentials{AccessKeyID: TestAccessKeyID, SecretAccessKey: TestSecretAccessKey}
	if err := v4.NewSigner().SignHTTP(context.Background(), creds, req, payloadHash, "s3", TestRegion, time.Now()); err != nil {
		b.Fatalf("signing request: %v", err)
	}
	return req
}

// BenchmarkServer measures whole requests through the server's handler:
// request ID, access log, metrics, SigV4 verification, the S3 handler and
// storage, without the network. The access log is written as JSON to
// io.Discard, so formatting it is included.
func BenchmarkServer(b *testing.B) {
	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer slog.SetDefault(defaultLogger)

	ts := NewTestServer(&testing.T{})
	defer ts.Close()
	handler := ts.Server.Config.Handler

	content := bytes.Repeat([]byte("x"), 1024)
	for i := range 100 {
		if _, err := ts.Storage.PutObject(TestBucket, fmt.Sprintf("dir/obj-%03d", i), "text/plain", nil, bytes.NewReader(content)); err != nil {
			b.Fatalf("PutObject: %v", err)
		}
	}

	run := func(b *testing.B, method, target string, body []byte, wantStatus int) {
		req := signedRequest(b, method, target, body)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			r := req.Clone(req.Context())
			// The server gives requests without a body http.NoBody
			r.Body = http.NoBody
			if len(body) > 0 {
				r.Body = io.NopCloser(bytes.NewReader(body))
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != wantStatus {
				b.Fatalf("%s %s: status %d, want %d: %s", method, target, w.Code, wantStatus, w.Body.String())
			}
		}
	}

	b.Run("HeadObject", func(b *testing.B) {
		run(b, "HEAD", "/"+TestBucket+"/dir/obj-001", nil, http.StatusOK)
	})
	b.Run("GetObject", func(b *testing.B) {
		run(b, "GET", "/"+TestBucket+"/dir/obj-001", nil, http.StatusOK)
	})
	b.Run("PutObject", func(b *testing.B) {
		run(b, "PUT", "/"+TestBucket+"/put-target", content, http.StatusOK)
	})
	b.Run("ListObjects", func(b *testing.B) {
		run(b, "GET", "/"+TestBucket+"?list-type=2&prefix=dir%2F", nil, http.StatusOK)
	})
}
