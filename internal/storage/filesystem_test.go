package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

func setupTestStorage(t *testing.T) (*FilesystemStorage, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "sss-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	basePath := filepath.Join(tmpDir, "data")
	multipartPath := filepath.Join(tmpDir, "multipart")

	storage, err := NewFilesystemStorage(basePath, multipartPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return storage, cleanup
}

func TestNewFilesystemStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sss-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	basePath := filepath.Join(tmpDir, "data")
	multipartPath := filepath.Join(tmpDir, "multipart")

	storage, err := NewFilesystemStorage(basePath, multipartPath)
	if err != nil {
		t.Fatalf("NewFilesystemStorage failed: %v", err)
	}

	if storage == nil {
		t.Fatal("storage is nil")
	}

	// Check directories were created
	objectsPath := filepath.Join(basePath, "objects")
	if _, err := os.Stat(objectsPath); os.IsNotExist(err) {
		t.Error("objects directory was not created")
	}

	if _, err := os.Stat(multipartPath); os.IsNotExist(err) {
		t.Error("multipart directory was not created")
	}
}

func TestPutAndGetObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "test/path/to/file.txt"
	content := []byte("Hello, World!")
	contentType := "text/plain"
	metadata := map[string]string{"author": "test"}

	// Put object
	meta, err := storage.PutObject(key, contentType, metadata, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if meta.Key != key {
		t.Errorf("Key = %q, want %q", meta.Key, key)
	}
	if meta.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", meta.Size, len(content))
	}
	if meta.ContentType != contentType {
		t.Errorf("ContentType = %q, want %q", meta.ContentType, contentType)
	}
	if !strings.HasPrefix(meta.ETag, "\"") || !strings.HasSuffix(meta.ETag, "\"") {
		t.Errorf("ETag should be quoted: %q", meta.ETag)
	}

	// Get object
	reader, getMeta, err := storage.GetObject(key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	gotContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading object content failed: %v", err)
	}

	if !bytes.Equal(gotContent, content) {
		t.Errorf("content = %q, want %q", gotContent, content)
	}

	if getMeta.ETag != meta.ETag {
		t.Errorf("ETag mismatch: get=%q, put=%q", getMeta.ETag, meta.ETag)
	}

	if getMeta.UserMetadata["author"] != "test" {
		t.Errorf("UserMetadata[author] = %q, want %q", getMeta.UserMetadata["author"], "test")
	}
}

func TestHeadObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "head-test.txt"
	content := []byte("Test content for head")

	// Put object first
	putMeta, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Head object
	headMeta, err := storage.HeadObject(key)
	if err != nil {
		t.Fatalf("HeadObject failed: %v", err)
	}

	if headMeta.Key != key {
		t.Errorf("Key = %q, want %q", headMeta.Key, key)
	}
	if headMeta.Size != putMeta.Size {
		t.Errorf("Size = %d, want %d", headMeta.Size, putMeta.Size)
	}
	if headMeta.ETag != putMeta.ETag {
		t.Errorf("ETag = %q, want %q", headMeta.ETag, putMeta.ETag)
	}
}

func TestHeadObjectNotFound(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	_, err := storage.HeadObject("nonexistent-key")
	if err == nil {
		t.Error("expected error for nonexistent object")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' error, got: %v", err)
	}
}

func TestDeleteObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "delete-test.txt"
	content := []byte("To be deleted")

	// Put object
	_, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify it exists
	exists, err := storage.ObjectExists(key)
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Error("object should exist after put")
	}

	// Delete object
	err = storage.DeleteObject(key)
	if err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify it's gone
	exists, err = storage.ObjectExists(key)
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("object should not exist after delete")
	}
}

func TestDeleteNonexistentObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	// Deleting nonexistent object should not error (S3 behavior)
	err := storage.DeleteObject("nonexistent-key")
	if err != nil {
		t.Errorf("DeleteObject on nonexistent key should not error: %v", err)
	}
}

func TestObjectExists(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "exists-test.txt"

	// Should not exist initially
	exists, err := storage.ObjectExists(key)
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if exists {
		t.Error("object should not exist initially")
	}

	// Put object
	_, err = storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("test")))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Should exist now
	exists, err = storage.ObjectExists(key)
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Error("object should exist after put")
	}
}

func TestPutObjectOverwrite(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "overwrite-test.txt"

	// Put initial content
	_, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("initial")))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Overwrite with new content
	newContent := []byte("overwritten content")
	meta, err := storage.PutObject(key, "text/html", nil, bytes.NewReader(newContent))
	if err != nil {
		t.Fatalf("PutObject (overwrite) failed: %v", err)
	}

	if meta.ContentType != "text/html" {
		t.Errorf("ContentType = %q, want %q", meta.ContentType, "text/html")
	}

	// Verify new content
	reader, _, err := storage.GetObject(key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	gotContent, _ := io.ReadAll(reader)
	if !bytes.Equal(gotContent, newContent) {
		t.Errorf("content = %q, want %q", gotContent, newContent)
	}
}

func TestKeyWithSpecialCharacters(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	keys := []string{
		"path/with spaces/file.txt",
		"unicode/文件/test.txt",
		"symbols/file@#$.txt",
		"deeply/nested/path/to/some/file.txt",
	}

	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			content := []byte("content for " + key)

			_, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader(content))
			if err != nil {
				t.Fatalf("PutObject failed for key %q: %v", key, err)
			}

			reader, _, err := storage.GetObject(key)
			if err != nil {
				t.Fatalf("GetObject failed for key %q: %v", key, err)
			}

			gotContent, _ := io.ReadAll(reader)
			reader.Close()

			if !bytes.Equal(gotContent, content) {
				t.Errorf("content mismatch for key %q", key)
			}
		})
	}
}

func TestEmptyObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "empty.txt"
	content := []byte{}

	meta, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if meta.Size != 0 {
		t.Errorf("Size = %d, want 0", meta.Size)
	}

	reader, getMeta, err := storage.GetObject(key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	gotContent, _ := io.ReadAll(reader)
	if len(gotContent) != 0 {
		t.Errorf("expected empty content, got %d bytes", len(gotContent))
	}

	if getMeta.Size != 0 {
		t.Errorf("getMeta.Size = %d, want 0", getMeta.Size)
	}
}

func TestLargeObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "large-file.bin"
	size := 10 * 1024 * 1024 // 10MB
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 256)
	}

	meta, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if meta.Size != int64(size) {
		t.Errorf("Size = %d, want %d", meta.Size, size)
	}

	reader, _, err := storage.GetObject(key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	gotContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("reading content failed: %v", err)
	}

	if !bytes.Equal(gotContent, content) {
		t.Error("content mismatch for large object")
	}
}

// Multipart upload tests

func TestMultipartUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "multipart-test.txt"
	contentType := "text/plain"

	// Create multipart upload
	uploadID, err := storage.CreateMultipartUpload(key, contentType, nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	if uploadID == "" {
		t.Fatal("uploadID is empty")
	}

	// Upload parts
	part1Content := []byte("Part 1 content. ")
	part2Content := []byte("Part 2 content. ")
	part3Content := []byte("Part 3 content.")

	part1Meta, err := storage.UploadPart(uploadID, 1, bytes.NewReader(part1Content))
	if err != nil {
		t.Fatalf("UploadPart 1 failed: %v", err)
	}

	part2Meta, err := storage.UploadPart(uploadID, 2, bytes.NewReader(part2Content))
	if err != nil {
		t.Fatalf("UploadPart 2 failed: %v", err)
	}

	part3Meta, err := storage.UploadPart(uploadID, 3, bytes.NewReader(part3Content))
	if err != nil {
		t.Fatalf("UploadPart 3 failed: %v", err)
	}

	// Complete upload
	parts := []s3.CompletedPartInput{
		{PartNumber: 1, ETag: part1Meta.ETag},
		{PartNumber: 2, ETag: part2Meta.ETag},
		{PartNumber: 3, ETag: part3Meta.ETag},
	}

	objMeta, err := storage.CompleteMultipartUpload(uploadID, parts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	expectedSize := int64(len(part1Content) + len(part2Content) + len(part3Content))
	if objMeta.Size != expectedSize {
		t.Errorf("Size = %d, want %d", objMeta.Size, expectedSize)
	}

	// Verify content
	reader, _, err := storage.GetObject(key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	expectedContent := append(append(part1Content, part2Content...), part3Content...)
	gotContent, _ := io.ReadAll(reader)

	if !bytes.Equal(gotContent, expectedContent) {
		t.Error("multipart content mismatch")
	}

	// ETag should have multipart format (hash-numparts)
	if !strings.Contains(objMeta.ETag, "-3") {
		t.Errorf("ETag should indicate 3 parts: %q", objMeta.ETag)
	}
}

func TestAbortMultipartUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "abort-test.txt"

	// Create upload
	uploadID, err := storage.CreateMultipartUpload(key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Upload a part
	_, err = storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("part content")))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	// Abort
	err = storage.AbortMultipartUpload(uploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify upload is gone
	_, err = storage.GetMultipartUpload(uploadID)
	if err == nil {
		t.Error("expected error getting aborted upload")
	}
}

func TestGetMultipartUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "get-upload-test.txt"
	contentType := "application/json"
	metadata := map[string]string{"custom": "value"}

	uploadID, err := storage.CreateMultipartUpload(key, contentType, metadata)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	uploadMeta, err := storage.GetMultipartUpload(uploadID)
	if err != nil {
		t.Fatalf("GetMultipartUpload failed: %v", err)
	}

	if uploadMeta.UploadID != uploadID {
		t.Errorf("UploadID = %q, want %q", uploadMeta.UploadID, uploadID)
	}
	if uploadMeta.Key != key {
		t.Errorf("Key = %q, want %q", uploadMeta.Key, key)
	}
	if uploadMeta.ContentType != contentType {
		t.Errorf("ContentType = %q, want %q", uploadMeta.ContentType, contentType)
	}
	if uploadMeta.UserMetadata["custom"] != "value" {
		t.Errorf("UserMetadata[custom] = %q, want %q", uploadMeta.UserMetadata["custom"], "value")
	}
}

func TestGetMultipartUploadNotFound(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	_, err := storage.GetMultipartUpload("nonexistent-upload-id")
	if err == nil {
		t.Error("expected error for nonexistent upload")
	}
}

func TestCompleteMultipartUploadInvalidPartOrder(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "order-test.txt"

	uploadID, err := storage.CreateMultipartUpload(key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	part1Meta, _ := storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("part 1")))
	part2Meta, _ := storage.UploadPart(uploadID, 2, bytes.NewReader([]byte("part 2")))

	// Parts in wrong order
	parts := []s3.CompletedPartInput{
		{PartNumber: 2, ETag: part2Meta.ETag},
		{PartNumber: 1, ETag: part1Meta.ETag},
	}

	_, err = storage.CompleteMultipartUpload(uploadID, parts)
	if err == nil {
		t.Error("expected error for parts not in ascending order")
	}
	if !strings.Contains(err.Error(), "order") {
		t.Errorf("expected order error, got: %v", err)
	}
}

func TestCompleteMultipartUploadMissingPart(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "missing-part-test.txt"

	uploadID, err := storage.CreateMultipartUpload(key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	part1Meta, _ := storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("part 1")))

	// Request part 1 and 2, but only uploaded part 1
	parts := []s3.CompletedPartInput{
		{PartNumber: 1, ETag: part1Meta.ETag},
		{PartNumber: 2, ETag: "\"fakeetag\""},
	}

	_, err = storage.CompleteMultipartUpload(uploadID, parts)
	if err == nil {
		t.Error("expected error for missing part")
	}
}

func TestListParts(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "list-parts-test.txt"

	uploadID, err := storage.CreateMultipartUpload(key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	// Upload parts out of order
	if _, err := storage.UploadPart(uploadID, 3, bytes.NewReader([]byte("part 3"))); err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}
	if _, err := storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("part 1"))); err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}
	if _, err := storage.UploadPart(uploadID, 2, bytes.NewReader([]byte("part 2"))); err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}

	parts, err := storage.ListParts(uploadID)
	if err != nil {
		t.Fatalf("ListParts failed: %v", err)
	}

	if len(parts) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(parts))
	}

	// Should be sorted by part number
	for i, part := range parts {
		expectedNum := i + 1
		if part.PartNumber != expectedNum {
			t.Errorf("part[%d].PartNumber = %d, want %d", i, part.PartNumber, expectedNum)
		}
	}
}

// Tests for ListObjects, CopyObject, and GetObjectRange

func TestListObjects(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	// Create test objects
	objects := []string{
		"dir1/file1.txt",
		"dir1/file2.txt",
		"dir1/subdir/file3.txt",
		"dir2/file4.txt",
		"root-file.txt",
	}

	for _, key := range objects {
		if _, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("content"))); err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
	}

	t.Run("list all objects", func(t *testing.T) {
		result, err := storage.ListObjects(ListObjectsOptions{})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(result.Objects) != 5 {
			t.Errorf("expected 5 objects, got %d", len(result.Objects))
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		result, err := storage.ListObjects(ListObjectsOptions{Prefix: "dir1/"})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects with prefix 'dir1/', got %d", len(result.Objects))
		}
	})

	t.Run("list with delimiter", func(t *testing.T) {
		result, err := storage.ListObjects(ListObjectsOptions{Delimiter: "/"})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		// Should have 1 root object and 2 common prefixes (dir1/, dir2/)
		if len(result.Objects) != 1 {
			t.Errorf("expected 1 root object, got %d", len(result.Objects))
		}
		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
		}
	})

	t.Run("list with prefix and delimiter", func(t *testing.T) {
		result, err := storage.ListObjects(ListObjectsOptions{Prefix: "dir1/", Delimiter: "/"})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		// Should have 2 files and 1 common prefix (dir1/subdir/)
		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}
		if len(result.CommonPrefixes) != 1 {
			t.Errorf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
		}
	})

	t.Run("list with max keys", func(t *testing.T) {
		result, err := storage.ListObjects(ListObjectsOptions{MaxKeys: 2})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}
		if !result.IsTruncated {
			t.Error("expected IsTruncated to be true")
		}
		if result.NextContinuationToken == "" {
			t.Error("expected NextContinuationToken to be set")
		}
	})

	t.Run("list with continuation token", func(t *testing.T) {
		// Get first page
		result1, _ := storage.ListObjects(ListObjectsOptions{MaxKeys: 2})

		// Get second page
		result2, err := storage.ListObjects(ListObjectsOptions{
			MaxKeys:           2,
			ContinuationToken: result1.NextContinuationToken,
		})
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}

		// Should have different objects
		if len(result2.Objects) == 0 {
			t.Error("expected objects in second page")
		}
		if len(result2.Objects) > 0 && result2.Objects[0].Key == result1.Objects[0].Key {
			t.Error("second page should have different objects")
		}
	})
}

func TestCopyObject(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	srcKey := "source.txt"
	dstKey := "destination.txt"
	content := []byte("content to copy")
	metadata := map[string]string{"author": "test"}

	// Create source object
	srcMeta, err := storage.PutObject(srcKey, "text/plain", metadata, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy object
	dstMeta, err := storage.CopyObject(srcKey, dstKey)
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Verify destination metadata
	if dstMeta.Size != srcMeta.Size {
		t.Errorf("Size = %d, want %d", dstMeta.Size, srcMeta.Size)
	}
	if dstMeta.ContentType != srcMeta.ContentType {
		t.Errorf("ContentType = %q, want %q", dstMeta.ContentType, srcMeta.ContentType)
	}

	// Verify destination content
	reader, _, err := storage.GetObject(dstKey)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	gotContent, _ := io.ReadAll(reader)
	reader.Close()

	if !bytes.Equal(gotContent, content) {
		t.Error("copied content doesn't match original")
	}

	// Verify source still exists
	exists, _ := storage.ObjectExists(srcKey)
	if !exists {
		t.Error("source object should still exist after copy")
	}
}

func TestCopyObjectNotFound(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	_, err := storage.CopyObject("nonexistent", "destination")
	if err == nil {
		t.Error("expected error when copying nonexistent object")
	}
}

func TestGetObjectRange(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	key := "range-test.txt"
	content := []byte("0123456789ABCDEFGHIJ") // 20 bytes

	_, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Run("full range", func(t *testing.T) {
		reader, _, err := storage.GetObjectRange(key, 0, 19)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		got, _ := io.ReadAll(reader)
		reader.Close()

		if !bytes.Equal(got, content) {
			t.Errorf("content = %q, want %q", got, content)
		}
	})

	t.Run("partial range from start", func(t *testing.T) {
		reader, _, err := storage.GetObjectRange(key, 0, 9)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		got, _ := io.ReadAll(reader)
		reader.Close()

		expected := []byte("0123456789")
		if !bytes.Equal(got, expected) {
			t.Errorf("content = %q, want %q", got, expected)
		}
	})

	t.Run("partial range from middle", func(t *testing.T) {
		reader, _, err := storage.GetObjectRange(key, 5, 14)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		got, _ := io.ReadAll(reader)
		reader.Close()

		expected := []byte("56789ABCDE")
		if !bytes.Equal(got, expected) {
			t.Errorf("content = %q, want %q", got, expected)
		}
	})

	t.Run("range past end", func(t *testing.T) {
		reader, _, err := storage.GetObjectRange(key, 15, 100)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		got, _ := io.ReadAll(reader)
		reader.Close()

		expected := []byte("FGHIJ")
		if !bytes.Equal(got, expected) {
			t.Errorf("content = %q, want %q", got, expected)
		}
	})

	t.Run("single byte", func(t *testing.T) {
		reader, _, err := storage.GetObjectRange(key, 5, 5)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		got, _ := io.ReadAll(reader)
		reader.Close()

		if len(got) != 1 || got[0] != '5' {
			t.Errorf("content = %q, want %q", got, "5")
		}
	})
}

func TestGetObjectRangeNotFound(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	_, _, err := storage.GetObjectRange("nonexistent", 0, 10)
	if err == nil {
		t.Error("expected error for nonexistent object")
	}
}

func TestCleanupStaleUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	t.Run("no uploads to clean", func(t *testing.T) {
		cleaned, err := storage.CleanupStaleUploads(time.Hour)
		if err != nil {
			t.Fatalf("CleanupStaleUploads failed: %v", err)
		}
		if cleaned != 0 {
			t.Errorf("cleaned = %d, want 0", cleaned)
		}
	})

	t.Run("cleans old uploads", func(t *testing.T) {
		// Create an upload
		uploadID, err := storage.CreateMultipartUpload("cleanup-test.txt", "text/plain", nil)
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		// Upload a part
		_, err = storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("test content")))
		if err != nil {
			t.Fatalf("UploadPart failed: %v", err)
		}

		// With a very short maxAge (0), it should clean up immediately
		cleaned, err := storage.CleanupStaleUploads(0)
		if err != nil {
			t.Fatalf("CleanupStaleUploads failed: %v", err)
		}
		if cleaned != 1 {
			t.Errorf("cleaned = %d, want 1", cleaned)
		}

		// Verify upload is gone
		_, err = storage.GetMultipartUpload(uploadID)
		if err == nil {
			t.Error("expected upload to be cleaned up")
		}
	})

	t.Run("keeps recent uploads", func(t *testing.T) {
		// Create an upload
		uploadID, err := storage.CreateMultipartUpload("keep-test.txt", "text/plain", nil)
		if err != nil {
			t.Fatalf("CreateMultipartUpload failed: %v", err)
		}

		// With a long maxAge, it should not clean up
		cleaned, err := storage.CleanupStaleUploads(24 * time.Hour)
		if err != nil {
			t.Fatalf("CleanupStaleUploads failed: %v", err)
		}
		if cleaned != 0 {
			t.Errorf("cleaned = %d, want 0", cleaned)
		}

		// Verify upload still exists
		_, err = storage.GetMultipartUpload(uploadID)
		if err != nil {
			t.Error("upload should still exist")
		}

		// Clean up for next test
		_ = storage.AbortMultipartUpload(uploadID)
	})

	t.Run("handles multiple uploads", func(t *testing.T) {
		// Create multiple uploads
		for i := 0; i < 3; i++ {
			uploadID, err := storage.CreateMultipartUpload("multi-cleanup-"+string(rune('a'+i))+".txt", "text/plain", nil)
			if err != nil {
				t.Fatalf("CreateMultipartUpload failed: %v", err)
			}
			_, _ = storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("content")))
		}

		// Clean all with maxAge 0
		cleaned, err := storage.CleanupStaleUploads(0)
		if err != nil {
			t.Fatalf("CleanupStaleUploads failed: %v", err)
		}
		if cleaned != 3 {
			t.Errorf("cleaned = %d, want 3", cleaned)
		}
	})

	t.Run("handles nonexistent multipart directory", func(t *testing.T) {
		// Create a fresh storage with a path that doesn't exist
		tmpDir, _ := os.MkdirTemp("", "sss-cleanup-test-*")
		defer os.RemoveAll(tmpDir)

		basePath := filepath.Join(tmpDir, "data")
		multipartPath := filepath.Join(tmpDir, "nonexistent-multipart")

		// Create storage (creates directories)
		s, _ := NewFilesystemStorage(basePath, multipartPath)

		// Remove the multipart directory
		os.RemoveAll(multipartPath)

		// Should handle gracefully
		cleaned, err := s.CleanupStaleUploads(time.Hour)
		if err != nil {
			t.Fatalf("CleanupStaleUploads should handle missing directory: %v", err)
		}
		if cleaned != 0 {
			t.Errorf("cleaned = %d, want 0", cleaned)
		}
	})
}
