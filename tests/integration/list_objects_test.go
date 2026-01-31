package integration

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// uploadObjects is a helper that uploads objects with the given keys and content.
func uploadObjects(t *testing.T, ctx context.Context, client *s3.Client, bucket string, keys []string) {
	t.Helper()
	for _, key := range keys {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("content-" + key)),
		})
		if err != nil {
			t.Fatalf("PutObject failed for %s: %v", key, err)
		}
	}
}

// TestListObjects_EmptyBucket tests listing an empty bucket returns no objects.
func TestListObjects_EmptyBucket(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(TestBucket),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if len(result.Contents) != 0 {
		t.Errorf("expected 0 objects in empty bucket, got %d", len(result.Contents))
	}
	if result.IsTruncated != nil && *result.IsTruncated {
		t.Error("expected IsTruncated to be false for empty bucket")
	}
	if result.KeyCount == nil || *result.KeyCount != 0 {
		t.Errorf("expected KeyCount 0, got %v", result.KeyCount)
	}
}

// TestListObjects_NonExistentBucket tests listing a bucket that doesn't exist.
func TestListObjects_NonExistentBucket(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	_, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("no-such-bucket"),
	})
	if err == nil {
		t.Fatal("expected error listing non-existent bucket")
	}
}

// TestListObjects_Delimiter tests that delimiter groups objects into common prefixes.
func TestListObjects_Delimiter(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{
		"photos/2023/jan/photo1.jpg",
		"photos/2023/jan/photo2.jpg",
		"photos/2023/feb/photo3.jpg",
		"photos/2024/mar/photo4.jpg",
		"documents/report.pdf",
		"readme.txt",
	}
	uploadObjects(t, ctx, client, TestBucket, keys)

	t.Run("root delimiter", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(TestBucket),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// Should have 1 object at root level: readme.txt
		if len(result.Contents) != 1 {
			t.Errorf("expected 1 root object, got %d", len(result.Contents))
			for _, obj := range result.Contents {
				t.Logf("  object: %s", *obj.Key)
			}
		} else if *result.Contents[0].Key != "readme.txt" {
			t.Errorf("expected root object readme.txt, got %s", *result.Contents[0].Key)
		}

		// Should have 2 common prefixes: "documents/" and "photos/"
		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
			for _, cp := range result.CommonPrefixes {
				t.Logf("  prefix: %s", *cp.Prefix)
			}
		}
	})

	t.Run("prefix with delimiter", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(TestBucket),
			Prefix:    aws.String("photos/"),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// No objects directly under photos/, only subdirectories
		if len(result.Contents) != 0 {
			t.Errorf("expected 0 objects under photos/, got %d", len(result.Contents))
		}

		// Should have 2 common prefixes: "photos/2023/" and "photos/2024/"
		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
			for _, cp := range result.CommonPrefixes {
				t.Logf("  prefix: %s", *cp.Prefix)
			}
		}
	})

	t.Run("deeper prefix with delimiter", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(TestBucket),
			Prefix:    aws.String("photos/2023/"),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// No objects directly under photos/2023/
		if len(result.Contents) != 0 {
			t.Errorf("expected 0 objects, got %d", len(result.Contents))
		}

		// Should have 2 common prefixes: "photos/2023/feb/" and "photos/2023/jan/"
		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
			for _, cp := range result.CommonPrefixes {
				t.Logf("  prefix: %s", *cp.Prefix)
			}
		}
	})

	t.Run("leaf prefix with delimiter returns objects", func(t *testing.T) {
		result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:    aws.String(TestBucket),
			Prefix:    aws.String("photos/2023/jan/"),
			Delimiter: aws.String("/"),
		})
		if err != nil {
			t.Fatalf("ListObjectsV2 failed: %v", err)
		}

		// 2 objects under photos/2023/jan/
		if len(result.Contents) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Contents))
		}
		if len(result.CommonPrefixes) != 0 {
			t.Errorf("expected 0 common prefixes, got %d", len(result.CommonPrefixes))
		}
	})
}

// TestListObjects_Pagination tests listing with continuation tokens for pagination.
func TestListObjects_Pagination(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	// Upload 5 objects with known sorted order
	keys := []string{"obj-a", "obj-b", "obj-c", "obj-d", "obj-e"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	// Fetch page by page with max-keys=2
	var allKeys []string
	var continuationToken *string

	for page := 0; page < 10; page++ { // safety limit
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(TestBucket),
			MaxKeys: aws.Int32(2),
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Fatalf("ListObjectsV2 page %d failed: %v", page, err)
		}

		for _, obj := range result.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
		if continuationToken == nil || *continuationToken == "" {
			t.Fatal("IsTruncated is true but NextContinuationToken is empty")
		}
	}

	if len(allKeys) != len(keys) {
		t.Fatalf("expected %d total objects across pages, got %d: %v", len(keys), len(allKeys), allKeys)
	}
	for i, key := range keys {
		if allKeys[i] != key {
			t.Errorf("page position %d: expected %s, got %s", i, key, allKeys[i])
		}
	}
}

// TestListObjects_StartAfter tests that StartAfter skips objects.
func TestListObjects_StartAfter(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:     aws.String(TestBucket),
		StartAfter: aws.String("bravo"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	// Should return charlie, delta, echo (3 objects)
	if len(result.Contents) != 3 {
		t.Errorf("expected 3 objects after 'bravo', got %d", len(result.Contents))
		for _, obj := range result.Contents {
			t.Logf("  object: %s", *obj.Key)
		}
	}
	if len(result.Contents) > 0 && *result.Contents[0].Key != "charlie" {
		t.Errorf("expected first object to be 'charlie', got %s", *result.Contents[0].Key)
	}
}

// TestListObjects_LexicographicOrder verifies objects are returned in sorted order.
func TestListObjects_LexicographicOrder(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	// Upload in non-sorted order
	keys := []string{"zebra", "apple", "mango", "banana", "cherry"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(TestBucket),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	expected := []string{"apple", "banana", "cherry", "mango", "zebra"}
	if len(result.Contents) != len(expected) {
		t.Fatalf("expected %d objects, got %d", len(expected), len(result.Contents))
	}
	for i, obj := range result.Contents {
		if *obj.Key != expected[i] {
			t.Errorf("position %d: expected %s, got %s", i, expected[i], *obj.Key)
		}
	}
}

// TestListObjects_ObjectMetadata verifies that listing returns correct object metadata.
func TestListObjects_ObjectMetadata(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	content := []byte("metadata-test-content")
	key := "metadata-check.txt"

	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(TestBucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(TestBucket),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if len(result.Contents) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Contents))
	}

	obj := result.Contents[0]
	if *obj.Key != key {
		t.Errorf("expected key %q, got %q", key, *obj.Key)
	}
	if obj.Size == nil || *obj.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %v", len(content), obj.Size)
	}
	if obj.ETag == nil || *obj.ETag == "" {
		t.Error("expected ETag to be set")
	}
	if obj.LastModified == nil || obj.LastModified.IsZero() {
		t.Error("expected LastModified to be set")
	}
}

// TestListObjects_PrefixNoMatch tests that a non-matching prefix returns no results.
func TestListObjects_PrefixNoMatch(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{"data/file1.txt", "data/file2.txt"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(TestBucket),
		Prefix: aws.String("nonexistent/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}
	if len(result.Contents) != 0 {
		t.Errorf("expected 0 objects for non-matching prefix, got %d", len(result.Contents))
	}
}

// TestListObjects_MaxKeysOne tests pagination works when fetching one object at a time.
func TestListObjects_MaxKeysOne(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{"one", "two", "three"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	var allKeys []string
	var continuationToken *string

	for page := 0; page < 10; page++ {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(TestBucket),
			MaxKeys: aws.Int32(1),
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Fatalf("ListObjectsV2 page %d failed: %v", page, err)
		}

		if len(result.Contents) != 1 {
			t.Errorf("page %d: expected 1 object, got %d", page, len(result.Contents))
		}
		for _, obj := range result.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	if len(allKeys) != 3 {
		t.Fatalf("expected 3 objects total, got %d: %v", len(allKeys), allKeys)
	}
}

// TestListObjects_PaginationWithPrefix tests that pagination works correctly with prefix filtering.
func TestListObjects_PaginationWithPrefix(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{
		"logs/2023-01.log",
		"logs/2023-02.log",
		"logs/2023-03.log",
		"logs/2023-04.log",
		"other/file.txt",
	}
	uploadObjects(t, ctx, client, TestBucket, keys)

	var allKeys []string
	var continuationToken *string

	for page := 0; page < 10; page++ {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(TestBucket),
			Prefix:  aws.String("logs/"),
			MaxKeys: aws.Int32(2),
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Fatalf("ListObjectsV2 page %d failed: %v", page, err)
		}

		for _, obj := range result.Contents {
			allKeys = append(allKeys, *obj.Key)
		}

		if result.IsTruncated == nil || !*result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	if len(allKeys) != 4 {
		t.Fatalf("expected 4 log objects, got %d: %v", len(allKeys), allKeys)
	}
	for _, k := range allKeys {
		if len(k) < 5 || k[:5] != "logs/" {
			t.Errorf("unexpected key without logs/ prefix: %s", k)
		}
	}
}

// TestListObjects_DelimiterNoDuplicatePrefixes verifies that common prefixes are deduplicated
// when multiple objects share the same prefix group.
func TestListObjects_DelimiterNoDuplicatePrefixes(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{
		"dir/a.txt",
		"dir/b.txt",
		"dir/c.txt",
	}
	uploadObjects(t, ctx, client, TestBucket, keys)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(TestBucket),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	// All 3 objects share the same prefix "dir/", so there should be exactly 1 common prefix
	if len(result.CommonPrefixes) != 1 {
		t.Errorf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
		for _, cp := range result.CommonPrefixes {
			t.Logf("  prefix: %s", *cp.Prefix)
		}
	}
	if len(result.CommonPrefixes) > 0 && *result.CommonPrefixes[0].Prefix != "dir/" {
		t.Errorf("expected common prefix 'dir/', got %s", *result.CommonPrefixes[0].Prefix)
	}
	// No objects at root level
	if len(result.Contents) != 0 {
		t.Errorf("expected 0 root objects, got %d", len(result.Contents))
	}
}

// TestListObjects_ResponseFields verifies the response includes expected top-level fields.
func TestListObjects_ResponseFields(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	client := ts.AWSClient(ctx)

	keys := []string{"field-test-1", "field-test-2"}
	uploadObjects(t, ctx, client, TestBucket, keys)

	result, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(TestBucket),
		Prefix:    aws.String("field-test"),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(10),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2 failed: %v", err)
	}

	if result.Name == nil || *result.Name != TestBucket {
		t.Errorf("expected Name %q, got %v", TestBucket, result.Name)
	}
	if result.Prefix == nil || *result.Prefix != "field-test" {
		t.Errorf("expected Prefix 'field-test', got %v", result.Prefix)
	}
	if result.Delimiter == nil || *result.Delimiter != "/" {
		t.Errorf("expected Delimiter '/', got %v", result.Delimiter)
	}
	if result.MaxKeys == nil || *result.MaxKeys != 10 {
		t.Errorf("expected MaxKeys 10, got %v", result.MaxKeys)
	}
	if result.KeyCount == nil || *result.KeyCount != 2 {
		t.Errorf("expected KeyCount 2, got %v", result.KeyCount)
	}
}
