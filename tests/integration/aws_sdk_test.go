package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestAWSSDK_CreateBucket tests bucket creation
func TestAWSSDK_CreateBucket(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	t.Run("create new bucket", func(t *testing.T) {
		bucketName := "new-aws-bucket"
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		// Verify bucket exists
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("HeadBucket after create failed: %v", err)
		}
	})

	t.Run("create existing bucket returns error", func(t *testing.T) {
		// TestBucket is already created
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(TestBucket),
		})
		if err == nil {
			t.Fatal("expected error when creating existing bucket")
		}
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") && !strings.Contains(err.Error(), "409") {
			t.Errorf("expected BucketAlreadyOwnedByYou error, got: %v", err)
		}
	})

	t.Run("create bucket with invalid name", func(t *testing.T) {
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String("INVALID"),
		})
		if err == nil {
			t.Fatal("expected error for invalid bucket name")
		}
	})

	t.Run("read-only credentials cannot create bucket", func(t *testing.T) {
		roClient := ts.AWSClientWithCreds(ctx, ReadOnlyAccessKeyID, ReadOnlySecretAccessKey)
		_, err := roClient.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String("should-fail-bucket"),
		})
		if err == nil {
			t.Fatal("expected error with read-only credentials")
		}
	})
}

// TestAWSSDK_DeleteBucket tests bucket deletion
func TestAWSSDK_DeleteBucket(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	t.Run("delete empty bucket", func(t *testing.T) {
		bucketName := "delete-me-aws"

		// Create bucket
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		// Delete bucket
		_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}

		// Verify bucket no longer exists
		_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("expected error for deleted bucket")
		}
	})

	t.Run("delete non-empty bucket fails", func(t *testing.T) {
		bucketName := "nonempty-aws-bucket"

		// Create bucket
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		// Add an object
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("test-file.txt"),
			Body:   bytes.NewReader([]byte("content")),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		// Try to delete - should fail
		_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("expected error when deleting non-empty bucket")
		}
		if !strings.Contains(err.Error(), "BucketNotEmpty") && !strings.Contains(err.Error(), "409") {
			t.Errorf("expected BucketNotEmpty error, got: %v", err)
		}
	})

	t.Run("delete non-existent bucket fails", func(t *testing.T) {
		_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String("nonexistent-aws-bucket"),
		})
		if err == nil {
			t.Fatal("expected error for non-existent bucket")
		}
		if !strings.Contains(err.Error(), "NoSuchBucket") && !strings.Contains(err.Error(), "404") {
			t.Errorf("expected NoSuchBucket error, got: %v", err)
		}
	})

	t.Run("read-only credentials cannot delete bucket", func(t *testing.T) {
		bucketName := "ro-delete-test"

		// Create bucket with RW creds
		_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		// Try to delete with RO creds
		roClient := ts.AWSClientWithCreds(ctx, ReadOnlyAccessKeyID, ReadOnlySecretAccessKey)
		_, err = roClient.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
		if err == nil {
			t.Fatal("expected error with read-only credentials")
		}
	})
}

// TestAWSSDK_HeadBucket tests bucket existence check
func TestAWSSDK_HeadBucket(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	t.Run("existing bucket", func(t *testing.T) {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(TestBucket),
		})
		if err != nil {
			t.Fatalf("HeadBucket failed: %v", err)
		}
	})

	t.Run("non-existing bucket", func(t *testing.T) {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String("non-existent-bucket"),
		})
		if err == nil {
			t.Fatal("expected error for non-existent bucket")
		}
	})
}

// TestAWSSDK_PutGetObject tests basic object upload and download
func TestAWSSDK_PutGetObject(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	content := []byte("Hello, World!")
	key := "test-file.txt"

	t.Run("put object", func(t *testing.T) {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(TestBucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(content),
			ContentType: aws.String("text/plain"),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	})

	t.Run("get object", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer result.Body.Close()

		data, err := io.ReadAll(result.Body)
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}

		if !bytes.Equal(data, content) {
			t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
		}

		if result.ContentType == nil || *result.ContentType != "text/plain" {
			t.Errorf("content-type mismatch: got %v, want text/plain", result.ContentType)
		}
	})

	t.Run("head object", func(t *testing.T) {
		result, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}

		if result.ContentLength == nil || *result.ContentLength != int64(len(content)) {
			t.Errorf("content-length mismatch: got %v, want %d", result.ContentLength, len(content))
		}

		if result.ETag == nil || *result.ETag == "" {
			t.Error("expected ETag to be set")
		}
	})
}

// TestAWSSDK_UpdateObject tests updating an existing object
func TestAWSSDK_UpdateObject(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "update-test.txt"
	originalContent := []byte("Original content")
	updatedContent := []byte("Updated content with more data")

	// Upload original
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(originalContent),
	})
	if err != nil {
		t.Fatalf("failed to upload original: %v", err)
	}

	// Update with new content
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(updatedContent),
	})
	if err != nil {
		t.Fatalf("failed to update: %v", err)
	}

	// Verify updated content
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("failed to get updated object: %v", err)
	}
	defer result.Body.Close()

	data, _ := io.ReadAll(result.Body)
	if !bytes.Equal(data, updatedContent) {
		t.Errorf("content not updated: got %q, want %q", string(data), string(updatedContent))
	}
}

// TestAWSSDK_DeleteObject tests object deletion
func TestAWSSDK_DeleteObject(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "delete-test.txt"

	// Upload
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("to be deleted")),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Delete
	_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify deleted
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err == nil {
		t.Fatal("expected error getting deleted object")
	}
}

// TestAWSSDK_DeleteObjects tests batch deletion
func TestAWSSDK_DeleteObjects(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{"batch-delete-1.txt", "batch-delete-2.txt", "batch-delete-3.txt"}

	// Upload files
	for _, key := range keys {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("content")),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", key, err)
		}
	}

	// Batch delete
	var objectIds []types.ObjectIdentifier
	for _, key := range keys {
		objectIds = append(objectIds, types.ObjectIdentifier{
			Key: aws.String(key),
		})
	}

	result, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(TestBucket),
		Delete: &types.Delete{
			Objects: objectIds,
		},
	})
	if err != nil {
		t.Fatalf("DeleteObjects failed: %v", err)
	}

	if len(result.Deleted) != len(keys) {
		t.Errorf("expected %d deleted, got %d", len(keys), len(result.Deleted))
	}

	// Verify all deleted
	for _, key := range keys {
		_, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
		})
		if err == nil {
			t.Errorf("expected error getting deleted object %s", key)
		}
	}
}

// TestAWSSDK_ListObjects tests listing objects
func TestAWSSDK_ListObjects(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	// Upload test files
	files := []string{"list/file1.txt", "list/file2.txt", "list/subdir/file3.txt", "other/file4.txt"}
	for _, key := range files {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("content")),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", key, err)
		}
	}

	t.Run("list all", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(TestBucket),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(result.Contents) != len(files) {
			t.Errorf("expected %d objects, got %d", len(files), len(result.Contents))
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(TestBucket),
			Prefix: aws.String("list/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(result.Contents) != 3 {
			t.Errorf("expected 3 objects with prefix 'list/', got %d", len(result.Contents))
		}
	})

	t.Run("list with max keys", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(TestBucket),
			MaxKeys: aws.Int32(2),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		if len(result.Contents) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Contents))
		}

		if !*result.IsTruncated {
			t.Error("expected IsTruncated to be true")
		}
	})
}

// TestAWSSDK_CopyObject tests copying objects
func TestAWSSDK_CopyObject(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	srcKey := "copy-source.txt"
	dstKey := "copy-dest.txt"
	content := []byte("content to copy")

	// Upload source
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(srcKey),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy
	_, err = client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(TestBucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(TestBucket + "/" + srcKey),
	})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify copy
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	data, _ := io.ReadAll(result.Body)
	if !bytes.Equal(data, content) {
		t.Errorf("copied content mismatch")
	}
}

// TestAWSSDK_RangeRequests tests partial content retrieval
func TestAWSSDK_RangeRequests(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "range-test.txt"
	content := []byte("0123456789ABCDEFGHIJ")

	// Upload
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Run("range bytes=0-4", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=0-4"),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer result.Body.Close()

		data, _ := io.ReadAll(result.Body)
		expected := "01234"
		if string(data) != expected {
			t.Errorf("got %q, want %q", string(data), expected)
		}
	})

	t.Run("range bytes=10-14", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=10-14"),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer result.Body.Close()

		data, _ := io.ReadAll(result.Body)
		expected := "ABCDE"
		if string(data) != expected {
			t.Errorf("got %q, want %q", string(data), expected)
		}
	})

	t.Run("range bytes=-5 (last 5 bytes)", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=-5"),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer result.Body.Close()

		data, _ := io.ReadAll(result.Body)
		expected := "FGHIJ"
		if string(data) != expected {
			t.Errorf("got %q, want %q", string(data), expected)
		}
	})

	t.Run("range bytes=15- (from byte 15 to end)", func(t *testing.T) {
		result, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String(key),
			Range:  aws.String("bytes=15-"),
		})
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer result.Body.Close()

		data, _ := io.ReadAll(result.Body)
		expected := "FGHIJ"
		if string(data) != expected {
			t.Errorf("got %q, want %q", string(data), expected)
		}
	})
}

// TestAWSSDK_PresignedDownload tests presigned URL for downloading
func TestAWSSDK_PresignedDownload(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)
	presignClient := ts.AWSPresignClient(ctx)

	key := "presigned-download.txt"
	content := []byte("presigned download content")

	// Upload file
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Generate presigned URL
	presignResult, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Hour))
	if err != nil {
		t.Fatalf("PresignGetObject failed: %v", err)
	}

	// Download using presigned URL (no auth headers)
	resp, err := http.Get(presignResult.URL)
	if err != nil {
		t.Fatalf("HTTP GET failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

// TestAWSSDK_PresignedUpload tests presigned URL for uploading
func TestAWSSDK_PresignedUpload(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)
	presignClient := ts.AWSPresignClient(ctx)

	key := "presigned-upload.txt"
	content := []byte("presigned upload content")

	// Generate presigned URL for upload
	presignResult, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Hour))
	if err != nil {
		t.Fatalf("PresignPutObject failed: %v", err)
	}

	// Upload using presigned URL (no auth headers)
	req, err := http.NewRequest(http.MethodPut, presignResult.URL, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HTTP PUT failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 OK, got %d: %s", resp.StatusCode, string(body))
	}

	// Verify upload
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	data, _ := io.ReadAll(result.Body)
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", string(data), string(content))
	}
}

// TestAWSSDK_MultipartUpload tests multipart upload workflow
func TestAWSSDK_MultipartUpload(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "multipart-test.bin"
	partSize := 5 * 1024 * 1024 // 5MB per part
	numParts := 3
	totalSize := partSize * numParts

	// Generate content
	content := GenerateContent(totalSize)

	// Create multipart upload
	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadID := createResult.UploadId

	// Upload parts
	var completedParts []types.CompletedPart
	for i := 0; i < numParts; i++ {
		partNum := int32(i + 1)
		start := i * partSize
		end := start + partSize
		partContent := content[start:end]

		uploadResult, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(TestBucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(partNum),
			Body:       bytes.NewReader(partContent),
		})
		if err != nil {
			t.Fatalf("UploadPart %d failed: %v", partNum, err)
		}

		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadResult.ETag,
			PartNumber: aws.Int32(partNum),
		})
	}

	// Complete multipart upload
	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(TestBucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify uploaded content
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Error("multipart upload content mismatch")
	}
}

// TestAWSSDK_AbortMultipartUpload tests aborting a multipart upload
func TestAWSSDK_AbortMultipartUpload(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "abort-multipart.bin"

	// Create multipart upload
	createResult, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Upload one part
	_, err = client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(TestBucket),
		Key:        aws.String(key),
		UploadId:   createResult.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader([]byte("part content")),
	})
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort
	_, err = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(TestBucket),
		Key:      aws.String(key),
		UploadId: createResult.UploadId,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify object doesn't exist
	_, err = client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err == nil {
		t.Fatal("expected error getting aborted upload")
	}
}

// TestAWSSDK_Authentication tests authentication scenarios
func TestAWSSDK_Authentication(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()

	t.Run("valid credentials", func(t *testing.T) {
		client := ts.AWSClient(ctx)
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(TestBucket),
		})
		if err != nil {
			t.Fatalf("HeadBucket with valid credentials failed: %v", err)
		}
	})

	t.Run("invalid access key", func(t *testing.T) {
		client := ts.AWSClientWithCreds(ctx, "INVALID", TestSecretAccessKey)
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(TestBucket),
		})
		if err == nil {
			t.Fatal("expected error with invalid access key")
		}
	})

	t.Run("invalid secret key", func(t *testing.T) {
		client := ts.AWSClientWithCreds(ctx, TestAccessKeyID, "invalid-secret")
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: aws.String(TestBucket),
		})
		if err == nil {
			t.Fatal("expected error with invalid secret key")
		}
	})

	t.Run("read-only credentials for read", func(t *testing.T) {
		// First upload a file with read-write creds
		rwClient := ts.AWSClient(ctx)
		_, err := rwClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String("readonly-test.txt"),
			Body:   bytes.NewReader([]byte("content")),
		})
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		// Read with read-only creds
		roClient := ts.AWSClientWithCreds(ctx, ReadOnlyAccessKeyID, ReadOnlySecretAccessKey)
		_, err = roClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String("readonly-test.txt"),
		})
		if err != nil {
			t.Fatalf("GetObject with read-only credentials failed: %v", err)
		}
	})

	t.Run("read-only credentials for write", func(t *testing.T) {
		client := ts.AWSClientWithCreds(ctx, ReadOnlyAccessKeyID, ReadOnlySecretAccessKey)
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(TestBucket),
			Key:    aws.String("should-fail.txt"),
			Body:   bytes.NewReader([]byte("content")),
		})
		if err == nil {
			t.Fatal("expected error with read-only credentials for write")
		}
	})
}

// TestAWSSDK_ResponseHeaders tests that response headers are correct
func TestAWSSDK_ResponseHeaders(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "headers-test.txt"
	content := []byte("test content for headers")

	// Upload with specific content type
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(TestBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Get object and check headers
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	// Verify headers
	if result.ContentLength == nil || *result.ContentLength != int64(len(content)) {
		t.Errorf("Content-Length mismatch: got %v, want %d", result.ContentLength, len(content))
	}

	if result.ContentType == nil || *result.ContentType != "application/octet-stream" {
		t.Errorf("Content-Type mismatch: got %v, want application/octet-stream", result.ContentType)
	}

	if result.ETag == nil || *result.ETag == "" {
		t.Error("ETag should be set")
	}

	if result.LastModified == nil {
		t.Error("Last-Modified should be set")
	}
}

// TestAWSSDK_SpecialCharacterKeys tests keys with special characters
func TestAWSSDK_SpecialCharacterKeys(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	testCases := []struct {
		name string
		key  string
	}{
		{"spaces", "file with spaces.txt"},
		{"unicode", "file-\u00e9\u00e8\u00ea.txt"},
		{"nested path", "deep/nested/path/to/file.txt"},
		{"special chars", "file+name=special&chars.txt"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			content := []byte("content for " + tc.key)

			// Upload
			_, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(TestBucket),
				Key:    aws.String(tc.key),
				Body:   bytes.NewReader(content),
			})
			if err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}

			// Download
			result, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(TestBucket),
				Key:    aws.String(tc.key),
			})
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}
			defer result.Body.Close()

			data, _ := io.ReadAll(result.Body)
			if !bytes.Equal(data, content) {
				t.Errorf("content mismatch for key %q", tc.key)
			}

			// Delete
			_, err = client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(TestBucket),
				Key:    aws.String(tc.key),
			})
			if err != nil {
				t.Fatalf("DeleteObject failed: %v", err)
			}
		})
	}
}

// TestAWSSDK_LargeFile tests uploading and downloading a larger file
func TestAWSSDK_LargeFile(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	key := "large-file.bin"
	size := 10 * 1024 * 1024 // 10MB
	content := GenerateContent(size)

	// Upload
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Download
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	if !bytes.Equal(data, content) {
		t.Error("large file content mismatch")
	}
}

// TestAWSSDK_NonExistentKey tests getting a non-existent key
func TestAWSSDK_NonExistentKey(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	_, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String("non-existent-key.txt"),
	})
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}

	// Check that error contains NoSuchKey
	if !strings.Contains(err.Error(), "NoSuchKey") && !strings.Contains(err.Error(), "404") {
		t.Errorf("expected NoSuchKey error, got: %v", err)
	}
}
