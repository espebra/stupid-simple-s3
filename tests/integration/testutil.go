package integration

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/espen/stupid-simple-s3/internal/api"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

const (
	TestBucket          = "test-bucket"
	TestAccessKeyID     = "AKIAIOSFODNN7EXAMPLE"
	TestSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	TestRegion          = "us-east-1"

	// Read-only credentials
	ReadOnlyAccessKeyID     = "AKIAIOSFODNN7READONLY"
	ReadOnlySecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYREADONLY"
)

// TestServer wraps a test HTTP server with S3 service
type TestServer struct {
	Server      *httptest.Server
	Config      *config.Config
	Storage     storage.MultipartStorage
	StoragePath string
	TempPath    string
}

// NewTestServer creates a new test server with temporary storage
func NewTestServer(t *testing.T) *TestServer {
	t.Helper()

	// Create temporary directories for storage
	storagePath := t.TempDir()
	tempPath := t.TempDir()

	cfg := &config.Config{
		Bucket: config.Bucket{
			Name: TestBucket,
		},
		Storage: config.Storage{
			Path:          storagePath,
			MultipartPath: tempPath,
		},
		Server: config.Server{
			Address: ":0", // Random port
		},
		Credentials: []config.Credential{
			{
				AccessKeyID:     TestAccessKeyID,
				SecretAccessKey: TestSecretAccessKey,
				Privileges:      "read-write",
			},
			{
				AccessKeyID:     ReadOnlyAccessKeyID,
				SecretAccessKey: ReadOnlySecretAccessKey,
				Privileges:      "read",
			},
		},
	}

	store, err := storage.NewFilesystemStorage(storagePath, tempPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// Create the test bucket
	if err := store.CreateBucket(TestBucket); err != nil {
		t.Fatalf("failed to create test bucket: %v", err)
	}

	srv := api.NewServer(cfg, store)
	testServer := httptest.NewServer(srv.Handler())

	return &TestServer{
		Server:      testServer,
		Config:      cfg,
		Storage:     store,
		StoragePath: storagePath,
		TempPath:    tempPath,
	}
}

// Close shuts down the test server
func (ts *TestServer) Close() {
	ts.Server.Close()
}

// URL returns the test server URL
func (ts *TestServer) URL() string {
	return ts.Server.URL
}

// AWSClient creates an AWS SDK v2 S3 client configured for the test server
func (ts *TestServer) AWSClient(ctx context.Context) *s3.Client {
	return ts.AWSClientWithCreds(ctx, TestAccessKeyID, TestSecretAccessKey)
}

// AWSClientWithCreds creates an AWS SDK v2 S3 client with specific credentials
func (ts *TestServer) AWSClientWithCreds(ctx context.Context, accessKeyID, secretAccessKey string) *s3.Client {
	return s3.New(s3.Options{
		Region:       TestRegion,
		BaseEndpoint: aws.String(ts.URL()),
		Credentials:  credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		UsePathStyle: true,
	})
}

// AWSPresignClient creates an AWS SDK v2 S3 presign client
func (ts *TestServer) AWSPresignClient(ctx context.Context) *s3.PresignClient {
	client := ts.AWSClient(ctx)
	return s3.NewPresignClient(client)
}

// AWSPresignClientWithCreds creates an AWS SDK v2 S3 presign client with specific credentials
func (ts *TestServer) AWSPresignClientWithCreds(ctx context.Context, accessKeyID, secretAccessKey string) *s3.PresignClient {
	client := ts.AWSClientWithCreds(ctx, accessKeyID, secretAccessKey)
	return s3.NewPresignClient(client)
}

// MinioClient creates a Minio client configured for the test server
func (ts *TestServer) MinioClient() (*minio.Client, error) {
	return ts.MinioClientWithCreds(TestAccessKeyID, TestSecretAccessKey)
}

// MinioClientWithCreds creates a Minio client with specific credentials
func (ts *TestServer) MinioClientWithCreds(accessKeyID, secretAccessKey string) (*minio.Client, error) {
	// Extract host from URL (remove http://)
	endpoint := ts.URL()[7:] // Remove "http://"

	return minio.New(endpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
		Region: TestRegion,
	})
}

// MinioCore creates a Minio Core client for low-level operations
func (ts *TestServer) MinioCore() (*minio.Core, error) {
	return ts.MinioCoreWithCreds(TestAccessKeyID, TestSecretAccessKey)
}

// MinioCoreWithCreds creates a Minio Core client with specific credentials
func (ts *TestServer) MinioCoreWithCreds(accessKeyID, secretAccessKey string) (*minio.Core, error) {
	endpoint := ts.URL()[7:]

	return minio.NewCore(endpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: false,
		Region: TestRegion,
	})
}

// Helper function to generate random content
func GenerateContent(size int) []byte {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}
	return content
}
