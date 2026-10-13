package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/espen/stupid-simple-s3/internal/api"
)

// checksumClient returns an AWS SDK client that talks HTTPS to the test server
// with the checksum defaults of config.LoadDefaultConfig. The SDK only sends
// trailing checksums (aws-chunked with x-amz-trailer) over HTTPS, which is how
// clients reach the service through Varnish in production.
func checksumClient(t *testing.T, ts *TestServer) *s3.Client {
	t.Helper()
	tlsServer := httptest.NewTLSServer(api.NewServer(ts.Config, ts.Storage).Handler())
	t.Cleanup(tlsServer.Close)
	return s3.New(s3.Options{
		Region:                     TestRegion,
		BaseEndpoint:               aws.String(tlsServer.URL),
		Credentials:                credentials.NewStaticCredentialsProvider(TestAccessKeyID, TestSecretAccessKey, ""),
		UsePathStyle:               true,
		HTTPClient:                 tlsServer.Client(),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenSupported,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenSupported,
	})
}

// TestAWSSDK_Checksums tests CRC32 checksums with the AWS SDK's default
// checksum behaviour: a trailing CRC32 on uploads and response validation on downloads.
func TestAWSSDK_Checksums(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := checksumClient(t, ts)

	content := []byte("checksummed content")
	key := "checksum.txt"

	t.Run("put returns checksum", func(t *testing.T) {
		result, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		if result.ChecksumCRC32 == nil || *result.ChecksumCRC32 == "" {
			t.Error("expected ChecksumCRC32 in PutObject response")
		}
	})

	t.Run("get returns and validates checksum", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:       aws.String(TestBucket),
			Key:          aws.String(key),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer func() { _ = result.Body.Close() }()

		data, err := io.ReadAll(result.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if !bytes.Equal(data, content) {
			t.Errorf("content mismatch: got %q, want %q", data, content)
		}
		if result.ChecksumCRC32 == nil || *result.ChecksumCRC32 == "" {
			t.Error("expected ChecksumCRC32 in GetObject response")
		}
	})

	t.Run("wrong checksum is rejected", func(t *testing.T) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(TestBucket),
			Key:           aws.String("bad-checksum.txt"),
			Body:          bytes.NewReader(content),
			ChecksumCRC32: aws.String("AAAAAA=="),
		})
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "BadDigest" {
			t.Fatalf("expected BadDigest error, got %v", err)
		}
	})

	t.Run("corrupted data fails client validation", func(t *testing.T) {
		corruptObjectData(t, ts.StoragePath)

		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:       aws.String(TestBucket),
			Key:          aws.String(key),
			ChecksumMode: types.ChecksumModeEnabled,
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer func() { _ = result.Body.Close() }()

		if _, err := io.ReadAll(result.Body); err == nil {
			t.Error("expected checksum validation error reading corrupted object")
		}
	})
}

// TestAWSSDK_MultipartChecksum tests that a multipart object's full-object
// CRC32 is accepted by the AWS SDK's response validation.
func TestAWSSDK_MultipartChecksum(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := checksumClient(t, ts)

	key := "multipart-checksum.bin"
	partSize := 5 * 1024 * 1024
	content := GenerateContent(partSize * 2)

	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	var completedParts []types.CompletedPart
	for i := 0; i < 2; i++ {
		partNum := int32(i + 1)
		uploadResult, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(TestBucket),
			Key:        aws.String(key),
			UploadId:   createResult.UploadId,
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(content[i*partSize : (i+1)*partSize]),
		})
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", partNum, err)
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(partNum),
		})
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(TestBucket),
		Key:             aws.String(key),
		UploadId:        createResult.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(TestBucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() { _ = result.Body.Close() }()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Error("multipart content mismatch")
	}
	if result.ChecksumType != types.ChecksumTypeFullObject {
		t.Errorf("ChecksumType = %q, want %q", result.ChecksumType, types.ChecksumTypeFullObject)
	}
}

// corruptObjectData flips the first byte of every object data file under storagePath
func corruptObjectData(t *testing.T, storagePath string) {
	t.Helper()
	err := filepath.WalkDir(storagePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "data" {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil || len(data) == 0 {
			return err
		}
		data[0] ^= 0xff
		return os.WriteFile(path, data, 0600)
	})
	if err != nil {
		t.Fatalf("corrupting object data: %v", err)
	}
}
