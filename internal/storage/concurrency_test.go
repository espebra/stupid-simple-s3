package storage

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// TestConcurrentUploads tests uploading multiple different files concurrently
func TestConcurrentUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numFiles = 50
	const fileSize = 1024 // 1KB each

	var wg sync.WaitGroup
	errors := make(chan error, numFiles)

	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			key := fmt.Sprintf("concurrent/upload-%d.bin", idx)
			content := make([]byte, fileSize)
			// Fill with unique pattern based on index
			for j := range content {
				content[j] = byte((idx + j) % 256)
			}

			_, err := storage.PutObject(testBucket, key, "application/octet-stream", nil, bytes.NewReader(content))
			if err != nil {
				errors <- fmt.Errorf("upload %d failed: %w", idx, err)
				return
			}

			// Verify the upload
			reader, _, err := storage.GetObject(testBucket, key)
			if err != nil {
				errors <- fmt.Errorf("get %d failed: %w", idx, err)
				return
			}
			got, _ := io.ReadAll(reader)
			reader.Close()

			if !bytes.Equal(got, content) {
				errors <- fmt.Errorf("content mismatch for file %d", idx)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentOverwrites tests multiple goroutines overwriting the same file
func TestConcurrentOverwrites(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numWriters = 20
	const iterations = 10
	const key = "concurrent/overwrite-target.txt"

	var wg sync.WaitGroup
	errors := make(chan error, numWriters*iterations)

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()

			for j := 0; j < iterations; j++ {
				content := []byte(fmt.Sprintf("writer-%d-iteration-%d", writerID, j))

				_, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader(content))
				if err != nil {
					errors <- fmt.Errorf("writer %d iteration %d failed: %w", writerID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify file exists and is readable
	reader, meta, err := storage.GetObject(testBucket, key)
	if err != nil {
		t.Fatalf("final GetObject failed: %v", err)
	}
	content, _ := io.ReadAll(reader)
	reader.Close()

	if meta.Size == 0 || len(content) == 0 {
		t.Error("final file is empty")
	}
}

// TestConcurrentDownloads tests multiple goroutines downloading the same file
func TestConcurrentDownloads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numReaders = 50
	const fileSize = 10 * 1024 // 10KB
	const key = "concurrent/download-source.bin"

	// Create the source file
	content := make([]byte, fileSize)
	for i := range content {
		content[i] = byte(i % 256)
	}

	_, err := storage.PutObject(testBucket, key, "application/octet-stream", nil, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			reader, meta, err := storage.GetObject(testBucket, key)
			if err != nil {
				errors <- fmt.Errorf("reader %d GetObject failed: %w", readerID, err)
				return
			}

			got, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				errors <- fmt.Errorf("reader %d read failed: %w", readerID, err)
				return
			}

			if meta.Size != int64(fileSize) {
				errors <- fmt.Errorf("reader %d: size mismatch, got %d want %d", readerID, meta.Size, fileSize)
				return
			}

			if !bytes.Equal(got, content) {
				errors <- fmt.Errorf("reader %d: content mismatch", readerID)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentDeletions tests deleting multiple files concurrently
func TestConcurrentDeletions(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numFiles = 50

	// Create files first
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("concurrent/delete-%d.txt", i)
		content := []byte(fmt.Sprintf("content for file %d", i))
		if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader(content)); err != nil {
			t.Fatalf("failed to create file %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, numFiles)

	for i := 0; i < numFiles; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			key := fmt.Sprintf("concurrent/delete-%d.txt", idx)
			if err := storage.DeleteObject(testBucket, key); err != nil {
				errors <- fmt.Errorf("delete %d failed: %w", idx, err)
				return
			}

			// Verify deletion
			exists, err := storage.ObjectExists(testBucket, key)
			if err != nil {
				errors <- fmt.Errorf("exists check %d failed: %w", idx, err)
				return
			}
			if exists {
				errors <- fmt.Errorf("file %d still exists after deletion", idx)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentMixedOperations tests uploads, downloads, and deletions happening simultaneously
func TestConcurrentMixedOperations(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numOperations = 30
	const fileSize = 1024

	// Pre-create some files for reading and deleting
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("mixed/existing-%d.txt", i)
		content := []byte(fmt.Sprintf("existing content %d", i))
		if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader(content)); err != nil {
			t.Fatalf("failed to create file %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errors := make(chan error, numOperations*3)

	// Writers - create new files
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("mixed/new-%d.bin", idx)
			content := make([]byte, fileSize)
			for j := range content {
				content[j] = byte((idx + j) % 256)
			}
			if _, err := storage.PutObject(testBucket, key, "application/octet-stream", nil, bytes.NewReader(content)); err != nil {
				errors <- fmt.Errorf("write %d failed: %w", idx, err)
			}
		}(i)
	}

	// Readers - read existing files
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("mixed/existing-%d.txt", idx)
			reader, _, err := storage.GetObject(testBucket, key)
			if err != nil {
				// File might have been deleted by another goroutine
				if err != ErrObjectNotFound {
					errors <- fmt.Errorf("read %d failed: %w", idx, err)
				}
				return
			}
			_, _ = io.ReadAll(reader)
			reader.Close()
		}(i)
	}

	// Deleters - delete existing files
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("mixed/existing-%d.txt", idx)
			// S3 delete is idempotent, should not error even if already deleted
			if err := storage.DeleteObject(testBucket, key); err != nil {
				errors <- fmt.Errorf("delete %d failed: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify new files exist
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("mixed/new-%d.bin", i)
		exists, err := storage.ObjectExists(testBucket, key)
		if err != nil {
			t.Errorf("exists check for new file %d failed: %v", i, err)
		}
		if !exists {
			t.Errorf("new file %d should exist", i)
		}
	}
}

// TestConcurrentMultipartUploads tests multiple multipart uploads happening simultaneously
func TestConcurrentMultipartUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numUploads = 10
	const partsPerUpload = 5
	const partSize = 1024

	var wg sync.WaitGroup
	errors := make(chan error, numUploads)

	for i := 0; i < numUploads; i++ {
		wg.Add(1)
		go func(uploadIdx int) {
			defer wg.Done()

			key := fmt.Sprintf("multipart/concurrent-%d.bin", uploadIdx)

			uploadID, err := storage.CreateMultipartUpload(testBucket, key, "application/octet-stream", nil)
			if err != nil {
				errors <- fmt.Errorf("create multipart %d failed: %w", uploadIdx, err)
				return
			}

			// Upload parts concurrently within this upload
			var partWg sync.WaitGroup
			partErrors := make(chan error, partsPerUpload)
			partETags := make([]string, partsPerUpload)
			var partMu sync.Mutex

			for p := 1; p <= partsPerUpload; p++ {
				partWg.Add(1)
				go func(partNum int) {
					defer partWg.Done()

					content := make([]byte, partSize)
					for j := range content {
						content[j] = byte((uploadIdx + partNum + j) % 256)
					}

					partMeta, err := storage.UploadPart(uploadID, partNum, bytes.NewReader(content))
					if err != nil {
						partErrors <- fmt.Errorf("upload %d part %d failed: %w", uploadIdx, partNum, err)
						return
					}

					partMu.Lock()
					partETags[partNum-1] = partMeta.ETag
					partMu.Unlock()
				}(p)
			}

			partWg.Wait()
			close(partErrors)

			for err := range partErrors {
				errors <- err
				return
			}

			// Complete the upload
			completedParts := make([]s3.CompletedPartInput, partsPerUpload)
			for p := 0; p < partsPerUpload; p++ {
				completedParts[p] = s3.CompletedPartInput{
					PartNumber: p + 1,
					ETag:       partETags[p],
				}
			}

			_, err = storage.CompleteMultipartUpload(uploadID, completedParts)
			if err != nil {
				errors <- fmt.Errorf("complete multipart %d failed: %w", uploadIdx, err)
				return
			}

			// Verify the completed object
			exists, err := storage.ObjectExists(testBucket, key)
			if err != nil {
				errors <- fmt.Errorf("exists check %d failed: %w", uploadIdx, err)
				return
			}
			if !exists {
				errors <- fmt.Errorf("completed object %d does not exist", uploadIdx)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestConcurrentReadsDuringWrite tests reading a file while it's being overwritten
func TestConcurrentReadsDuringWrite(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const key = "concurrent/read-during-write.txt"
	const numReaders = 20
	const numWrites = 20

	// Create initial file
	initialContent := []byte("initial content")
	if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader(initialContent)); err != nil {
		t.Fatalf("failed to create initial file: %v", err)
	}

	var wg sync.WaitGroup
	var readErrors, writeErrors int64

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				reader, _, err := storage.GetObject(testBucket, key)
				if err != nil {
					// Object might be in transition, this is expected
					atomic.AddInt64(&readErrors, 1)
					continue
				}
				content, err := io.ReadAll(reader)
				reader.Close()
				if err != nil {
					atomic.AddInt64(&readErrors, 1)
					continue
				}
				// Content should be non-empty and consistent
				if len(content) == 0 {
					atomic.AddInt64(&readErrors, 1)
				}
			}
		}()
	}

	// Start writers
	for i := 0; i < numWrites; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			content := []byte(fmt.Sprintf("updated content version %d with some padding to make it longer", idx))
			if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader(content)); err != nil {
				atomic.AddInt64(&writeErrors, 1)
			}
		}(i)
	}

	wg.Wait()

	// Some read errors during concurrent writes are acceptable
	// but all writes should succeed
	if writeErrors > 0 {
		t.Errorf("had %d write errors, expected 0", writeErrors)
	}

	// Verify file is in consistent state after all operations
	reader, _, err := storage.GetObject(testBucket, key)
	if err != nil {
		t.Fatalf("final GetObject failed: %v", err)
	}
	finalContent, _ := io.ReadAll(reader)
	reader.Close()

	if len(finalContent) == 0 {
		t.Error("final file is empty")
	}
}

// TestConcurrentMultipartPartsOnSameUpload tests uploading parts concurrently to the same multipart upload
func TestConcurrentMultipartPartsOnSameUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numParts = 20
	const partSize = 1024
	const key = "multipart/concurrent-parts.bin"

	uploadID, err := storage.CreateMultipartUpload(testBucket, key, "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	var wg sync.WaitGroup
	errors := make(chan error, numParts)
	partETags := make([]string, numParts)
	var mu sync.Mutex

	for i := 1; i <= numParts; i++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()

			content := make([]byte, partSize)
			for j := range content {
				content[j] = byte((partNum + j) % 256)
			}

			partMeta, err := storage.UploadPart(uploadID, partNum, bytes.NewReader(content))
			if err != nil {
				errors <- fmt.Errorf("part %d failed: %w", partNum, err)
				return
			}

			mu.Lock()
			partETags[partNum-1] = partMeta.ETag
			mu.Unlock()
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Complete the upload
	completedParts := make([]s3.CompletedPartInput, numParts)
	for i := 0; i < numParts; i++ {
		completedParts[i] = s3.CompletedPartInput{
			PartNumber: i + 1,
			ETag:       partETags[i],
		}
	}

	objMeta, err := storage.CompleteMultipartUpload(uploadID, completedParts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	expectedSize := int64(numParts * partSize)
	if objMeta.Size != expectedSize {
		t.Errorf("Size = %d, want %d", objMeta.Size, expectedSize)
	}
}
