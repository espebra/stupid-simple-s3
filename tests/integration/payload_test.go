package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
)

// tamperingTransport changes request bodies after the client has signed
// them, as an attacker between the TLS proxy and the service could. It
// records the X-Amz-Content-Sha256 header of the last tampered request.
type tamperingTransport struct {
	tamper func([]byte) []byte

	mu            sync.Mutex
	contentSHA256 string
}

func (tt *tamperingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && (req.Method == http.MethodPut || req.Method == http.MethodPost) {
		body, err := io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
		body = tt.tamper(body)
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		tt.mu.Lock()
		tt.contentSHA256 = req.Header.Get("X-Amz-Content-Sha256")
		tt.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(req)
}

func (tt *tamperingTransport) lastContentSHA256() string {
	tt.mu.Lock()
	defer tt.mu.Unlock()
	return tt.contentSHA256
}

func tamperingAWSClient(ts *TestServer, tt *tamperingTransport) *s3.Client {
	return s3.New(s3.Options{
		Region:       TestRegion,
		BaseEndpoint: aws.String(ts.URL()),
		Credentials:  credentials.NewStaticCredentialsProvider(TestAccessKeyID, TestSecretAccessKey, ""),
		UsePathStyle: true,
		HTTPClient:   &http.Client{Transport: tt},
	})
}

func assertAPIErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != want {
		t.Fatalf("expected %s error, got %v", want, err)
	}
}

// TestPayloadHashVerified tests that a body changed after signing is
// rejected when the request signs its SHA-256 in X-Amz-Content-Sha256
func TestPayloadHashVerified(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()
	ctx := context.Background()
	client := ts.AWSClient(ctx)

	t.Run("PutObject", func(t *testing.T) {
		tt := &tamperingTransport{tamper: func(b []byte) []byte { return bytes.Replace(b, []byte("original"), []byte("tampered"), 1) }}
		_, err := tamperingAWSClient(ts, tt).PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String("tampered.txt"),
			Body:   strings.NewReader("original content"),
		})
		if h := tt.lastContentSHA256(); len(h) != 64 {
			t.Fatalf("the SDK did not sign the payload hash (X-Amz-Content-Sha256: %q)", h)
		}
		assertAPIErrorCode(t, err, "XAmzContentSHA256Mismatch")
		if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(TestBucket), Key: aws.String("tampered.txt")}); err == nil {
			t.Error("tampered object was stored")
		}
	})

	t.Run("DeleteObjects", func(t *testing.T) {
		for _, key := range []string{"kill-me", "keep-me"} {
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(TestBucket), Key: aws.String(key), Body: strings.NewReader("data")}); err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}
		}
		// Redirect the delete to another object of the same name length
		tt := &tamperingTransport{tamper: func(b []byte) []byte { return bytes.Replace(b, []byte("kill-me"), []byte("keep-me"), 1) }}
		_, err := tamperingAWSClient(ts, tt).DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(TestBucket),
			Delete: &types.Delete{Objects: []types.ObjectIdentifier{{Key: aws.String("kill-me")}}},
		})
		if h := tt.lastContentSHA256(); len(h) != 64 {
			t.Fatalf("the SDK did not sign the payload hash (X-Amz-Content-Sha256: %q)", h)
		}
		assertAPIErrorCode(t, err, "XAmzContentSHA256Mismatch")
		for _, key := range []string{"kill-me", "keep-me"} {
			if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(TestBucket), Key: aws.String(key)}); err != nil {
				t.Errorf("%s was deleted by a rejected request: %v", key, err)
			}
		}
	})
}

// TestChunkSignaturesVerified tests that a signed aws-chunked upload whose
// chunk data was changed after signing is rejected
func TestChunkSignaturesVerified(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	// Flip the first data byte after the first chunk header
	tt := &tamperingTransport{tamper: func(b []byte) []byte {
		if i := bytes.Index(b, []byte("\r\n")); i >= 0 && i+2 < len(b) {
			b[i+2] ^= 0xff
		}
		return b
	}}
	mc, err := minio.New(ts.URL()[len("http://"):], &minio.Options{
		Creds:     miniocreds.NewStaticV4(TestAccessKeyID, TestSecretAccessKey, ""),
		Region:    TestRegion,
		Transport: tt,
	})
	if err != nil {
		t.Fatalf("creating MinIO client: %v", err)
	}

	ctx := context.Background()
	content := GenerateContent(100 * 1024)
	_, err = mc.PutObject(ctx, TestBucket, "tampered-chunks.bin", bytes.NewReader(content), int64(len(content)), minio.PutObjectOptions{})
	if h := tt.lastContentSHA256(); h != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		t.Fatalf("the MinIO client did not send a signed aws-chunked upload (X-Amz-Content-Sha256: %q)", h)
	}
	if minio.ToErrorResponse(err).Code != "SignatureDoesNotMatch" {
		t.Fatalf("expected SignatureDoesNotMatch, got %v", err)
	}
	if _, err := ts.Storage.HeadObject(TestBucket, "tampered-chunks.bin"); err == nil {
		t.Error("object with a tampered chunk was stored")
	}
}
