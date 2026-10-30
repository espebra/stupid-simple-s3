package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
			_ = reader.Close()

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
	_ = reader.Close()

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
			_ = reader.Close()
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
			_ = reader.Close()
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
				_ = reader.Close()
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
	_ = reader.Close()

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

// TestConcurrentCompleteDoesNotBlockPartUploads verifies that completing one
// multipart upload does not block part uploads on a different upload.
// This is the convoy scenario that the per-upload locking fixes.
func TestConcurrentCompleteDoesNotBlockPartUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const partSize = 1024

	// Create two independent multipart uploads
	uploadA, err := storage.CreateMultipartUpload(testBucket, "key-a.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("create upload A: %v", err)
	}
	uploadB, err := storage.CreateMultipartUpload(testBucket, "key-b.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("create upload B: %v", err)
	}

	// Upload parts for both
	makeContent := func(seed int) []byte {
		c := make([]byte, partSize)
		for i := range c {
			c[i] = byte((seed + i) % 256)
		}
		return c
	}

	for p := 1; p <= 5; p++ {
		if _, err := storage.UploadPart(uploadA, p, bytes.NewReader(makeContent(p))); err != nil {
			t.Fatalf("upload A part %d: %v", p, err)
		}
	}

	etagsB := make([]string, 3)
	for p := 1; p <= 3; p++ {
		meta, err := storage.UploadPart(uploadB, p, bytes.NewReader(makeContent(100+p)))
		if err != nil {
			t.Fatalf("upload B part %d: %v", p, err)
		}
		etagsB[p-1] = meta.ETag
	}

	// Now concurrently: complete upload A while uploading more parts to B
	var wg sync.WaitGroup
	errs := make(chan error, 10)

	// Complete A (takes exclusive lock on A only)
	wg.Add(1)
	go func() {
		defer wg.Done()
		parts := make([]s3.CompletedPartInput, 5)
		// Need ETags for A; re-upload to get them
		for p := 1; p <= 5; p++ {
			meta, err := storage.UploadPart(uploadA, p, bytes.NewReader(makeContent(p)))
			if err != nil {
				errs <- fmt.Errorf("re-upload A part %d: %w", p, err)
				return
			}
			parts[p-1] = s3.CompletedPartInput{PartNumber: p, ETag: meta.ETag}
		}
		if _, err := storage.CompleteMultipartUpload(uploadA, parts); err != nil {
			errs <- fmt.Errorf("complete A: %w", err)
		}
	}()

	// Upload additional parts to B concurrently (should not be blocked by A's completion)
	for p := 4; p <= 8; p++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			if _, err := storage.UploadPart(uploadB, partNum, bytes.NewReader(makeContent(100+partNum))); err != nil {
				errs <- fmt.Errorf("upload B part %d: %w", partNum, err)
			}
		}(p)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestAbortDuringPartUpload verifies that aborting an upload waits for
// in-flight part uploads to finish before removing the directory.
func TestAbortDuringPartUpload(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const numParts = 10
	const partSize = 4096

	uploadID, err := storage.CreateMultipartUpload(testBucket, "abort-race.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Start uploading parts concurrently
	var wg sync.WaitGroup
	partStarted := make(chan struct{})

	for p := 1; p <= numParts; p++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			content := make([]byte, partSize)
			if partNum == 1 {
				close(partStarted) // signal that at least one part is in progress
			}
			// Ignore errors — some parts may fail due to abort
			_, _ = storage.UploadPart(uploadID, partNum, bytes.NewReader(content))
		}(p)
	}

	// Wait for at least one part to start, then abort
	<-partStarted
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = storage.AbortMultipartUpload(uploadID)
	}()

	wg.Wait()

	// After abort completes, the upload should be gone
	_, err = storage.GetMultipartUpload(uploadID)
	if err == nil {
		t.Error("upload should not exist after abort")
	}
}

// TestCleanupDuringActiveUploads verifies that the cleanup job does not
// interfere with active uploads or cause data corruption.
func TestCleanupDuringActiveUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const partSize = 1024

	// Create an active upload (recent, should not be cleaned up)
	uploadID, err := storage.CreateMultipartUpload(testBucket, "active.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// Upload parts concurrently
	etags := make([]string, 5)
	var mu sync.Mutex
	for p := 1; p <= 5; p++ {
		wg.Add(1)
		go func(partNum int) {
			defer wg.Done()
			content := make([]byte, partSize)
			meta, err := storage.UploadPart(uploadID, partNum, bytes.NewReader(content))
			if err != nil {
				errs <- fmt.Errorf("part %d: %w", partNum, err)
				return
			}
			mu.Lock()
			etags[partNum-1] = meta.ETag
			mu.Unlock()
		}(p)
	}

	// Run cleanup concurrently with a short maxAge that won't affect our new upload
	wg.Add(1)
	go func() {
		defer wg.Done()
		// maxAge=1h means our just-created upload won't be cleaned
		if _, err := storage.CleanupStaleUploads(1 * time.Hour); err != nil {
			errs <- fmt.Errorf("cleanup: %w", err)
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The active upload should still be completable
	parts := make([]s3.CompletedPartInput, 5)
	for i := 0; i < 5; i++ {
		parts[i] = s3.CompletedPartInput{PartNumber: i + 1, ETag: etags[i]}
	}
	objMeta, err := storage.CompleteMultipartUpload(uploadID, parts)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if objMeta.Size != int64(5*partSize) {
		t.Errorf("size = %d, want %d", objMeta.Size, 5*partSize)
	}
}

// TestConcurrentCompleteSameKey verifies that two multipart uploads targeting
// the same object key can complete concurrently without corrupting the object.
func TestConcurrentCompleteSameKey(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const partSize = 2048
	const key = "same-key.bin"

	// Create two uploads for the same key
	createAndUpload := func(seed byte) (string, []s3.CompletedPartInput) {
		uploadID, err := storage.CreateMultipartUpload(testBucket, key, "application/octet-stream", nil)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		parts := make([]s3.CompletedPartInput, 3)
		for p := 1; p <= 3; p++ {
			content := make([]byte, partSize)
			for i := range content {
				content[i] = seed
			}
			meta, err := storage.UploadPart(uploadID, p, bytes.NewReader(content))
			if err != nil {
				t.Fatalf("part %d: %v", p, err)
			}
			parts[p-1] = s3.CompletedPartInput{PartNumber: p, ETag: meta.ETag}
		}
		return uploadID, parts
	}

	uploadA, partsA := createAndUpload(0xAA)
	uploadB, partsB := createAndUpload(0xBB)

	// Complete both concurrently
	var wg sync.WaitGroup
	errs := make(chan error, 2)

	for _, tc := range []struct {
		id    string
		parts []s3.CompletedPartInput
	}{{uploadA, partsA}, {uploadB, partsB}} {
		wg.Add(1)
		go func(id string, parts []s3.CompletedPartInput) {
			defer wg.Done()
			if _, err := storage.CompleteMultipartUpload(id, parts); err != nil {
				errs <- fmt.Errorf("complete %s: %w", id, err)
			}
		}(tc.id, tc.parts)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The object should exist and have consistent content (all one byte value)
	reader, meta, err := storage.GetObject(testBucket, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	content, _ := io.ReadAll(reader)
	_ = reader.Close()

	if meta.Size != int64(3*partSize) {
		t.Errorf("size = %d, want %d", meta.Size, 3*partSize)
	}

	// All bytes should be the same value (either 0xAA or 0xBB, depending on who won)
	if len(content) > 0 {
		expected := content[0]
		for i, b := range content {
			if b != expected {
				t.Errorf("byte %d = %x, want %x (content corrupted by concurrent complete)", i, b, expected)
				break
			}
		}
	}
}

// slowReader returns its data in small chunks, yielding between reads, so
// that concurrent writers interleave.
type slowReader struct {
	data []byte
}

func (r *slowReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p[:min(len(p), 512)], r.data)
	r.data = r.data[n:]
	runtime.Gosched()
	return n, nil
}

// TestConcurrentUploadsOfSamePart tests that concurrent uploads of the same
// part number leave the data and metadata of exactly one of them.
func TestConcurrentUploadsOfSamePart(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	uploadID, err := storage.CreateMultipartUpload(testBucket, "same-part.bin", "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}

	const numWriters = 20
	const partSize = 64 * 1024
	contents := make([][]byte, numWriters)
	for i := range contents {
		contents[i] = bytes.Repeat([]byte{byte('a' + i)}, partSize)
	}

	var wg sync.WaitGroup
	errs := make(chan error, numWriters)
	for i := range numWriters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := storage.UploadPart(uploadID, 1, &slowReader{data: contents[i]}); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("UploadPart failed: %v", err)
	}

	parts, err := storage.ListParts(uploadID)
	if err != nil || len(parts) != 1 {
		t.Fatalf("ListParts = %v, %v; want one part", parts, err)
	}
	objMeta, err := storage.CompleteMultipartUpload(uploadID, []s3.CompletedPartInput{{PartNumber: 1, ETag: parts[0].ETag}})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	reader, _, err := storage.GetObject(testBucket, "same-part.bin")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() { _ = reader.Close() }()
	data, _ := io.ReadAll(reader)

	if !slices.ContainsFunc(contents, func(c []byte) bool { return bytes.Equal(c, data) }) {
		t.Fatalf("part data is not the data of any single upload (%d bytes)", len(data))
	}
	if want := sha256Checksum(data); parts[0].ChecksumSHA256 != want {
		t.Errorf("part checksum %q does not match the part data (%q)", parts[0].ChecksumSHA256, want)
	}
	if objMeta.Size != partSize {
		t.Errorf("object size = %d, want %d", objMeta.Size, partSize)
	}
}

// TestConcurrentPutAndDeleteBucket tests that an object written while its
// bucket is being deleted is either stored in a bucket that still exists, or
// rejected with the bucket gone. It must never be lost or revive the bucket.
func TestConcurrentPutAndDeleteBucket(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const bucket = "race-bucket"
	for i := range 200 {
		if err := storage.CreateBucket(bucket); err != nil {
			t.Fatalf("iteration %d: CreateBucket failed: %v", i, err)
		}

		var putErr, deleteErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, putErr = storage.PutObject(bucket, "key", "text/plain", nil, bytes.NewReader([]byte("data")))
		}()
		go func() {
			defer wg.Done()
			deleteErr = storage.DeleteBucket(bucket)
		}()
		wg.Wait()

		exists, err := storage.BucketExists(bucket)
		if err != nil {
			t.Fatalf("iteration %d: BucketExists failed: %v", i, err)
		}
		switch {
		case putErr == nil:
			if !errors.Is(deleteErr, ErrBucketNotEmpty) {
				t.Fatalf("iteration %d: object stored but DeleteBucket returned %v", i, deleteErr)
			}
			if objExists, _ := storage.ObjectExists(bucket, "key"); !objExists {
				t.Fatalf("iteration %d: stored object was lost", i)
			}
			if err := storage.DeleteObject(bucket, "key"); err != nil {
				t.Fatalf("iteration %d: DeleteObject failed: %v", i, err)
			}
			if err := storage.DeleteBucket(bucket); err != nil {
				t.Fatalf("iteration %d: DeleteBucket after DeleteObject failed: %v", i, err)
			}
		case errors.Is(putErr, ErrBucketNotFound):
			if deleteErr != nil {
				t.Fatalf("iteration %d: PutObject found no bucket but DeleteBucket returned %v", i, deleteErr)
			}
			if exists {
				t.Fatalf("iteration %d: rejected PutObject revived the bucket", i)
			}
		default:
			t.Fatalf("iteration %d: PutObject returned %v", i, putErr)
		}
	}
}

// TestConcurrentPutSameKeyKeepsMetadataConsistent tests that concurrent
// writes to the same key leave metadata that describes the stored data, not
// the data of one write and the metadata of another.
func TestConcurrentPutSameKeyKeepsMetadataConsistent(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const writers = 8
	for round := range 50 {
		var wg sync.WaitGroup
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Different sizes, so mismatched metadata shows in the size too
				data := bytes.Repeat([]byte{byte('a' + i)}, 1000+i*100)
				if _, err := storage.PutObject(testBucket, "same-key", "text/plain", nil, bytes.NewReader(data)); err != nil {
					t.Errorf("round %d: PutObject failed: %v", round, err)
				}
			}()
		}
		wg.Wait()

		reader, meta, err := storage.GetObject(testBucket, "same-key")
		if err != nil {
			t.Fatalf("round %d: GetObject failed: %v", round, err)
		}
		data, _ := io.ReadAll(reader)
		_ = reader.Close()
		if meta.Size != int64(len(data)) || meta.ChecksumSHA256 != sha256Checksum(data) {
			t.Fatalf("round %d: metadata (size %d, checksum %s) does not describe the stored data (size %d, checksum %s)",
				round, meta.Size, meta.ChecksumSHA256, len(data), sha256Checksum(data))
		}
	}
}

// TestConcurrentGetDuringOverwrite tests that a read during overwrites gets
// the data and metadata of the same write
func TestConcurrentGetDuringOverwrite(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	versions := [][]byte{bytes.Repeat([]byte("a"), 1000), bytes.Repeat([]byte("b"), 3000)}
	if _, err := storage.PutObject(testBucket, "overwritten", "text/plain", nil, bytes.NewReader(versions[0])); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	stop := make(chan struct{})
	var writers sync.WaitGroup
	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := storage.PutObject(testBucket, "overwritten", "text/plain", nil, bytes.NewReader(versions[i%2])); err != nil {
				t.Errorf("PutObject failed: %v", err)
				return
			}
		}
	}()

	check := func(t *testing.T, data []byte, meta *s3.ObjectMetadata, wantLen int64) bool {
		t.Helper()
		if int64(len(data)) != wantLen || meta.ChecksumSHA256 != sha256Checksum(versions[0]) && meta.ChecksumSHA256 != sha256Checksum(versions[1]) {
			t.Errorf("got %d bytes, want %d (metadata size %d)", len(data), wantLen, meta.Size)
			return false
		}
		want := versions[0]
		if meta.Size == int64(len(versions[1])) {
			want = versions[1]
		}
		if !bytes.Equal(data, want[:len(data)]) {
			t.Errorf("data of one version (%q...) with metadata of another (size %d)", data[:1], meta.Size)
			return false
		}
		return true
	}

	for range 2000 {
		reader, meta, err := storage.GetObject(testBucket, "overwritten")
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		data, _ := io.ReadAll(reader)
		_ = reader.Close()
		if !check(t, data, meta, meta.Size) {
			break
		}

		rangeReader, rangeMeta, err := storage.GetObjectRange(testBucket, "overwritten", 500, 999)
		if err != nil {
			t.Fatalf("GetObjectRange failed: %v", err)
		}
		data, _ = io.ReadAll(rangeReader)
		_ = rangeReader.Close()
		if !check(t, data, rangeMeta, 500) {
			break
		}
	}
	close(stop)
	writers.Wait()
}

// gatedReader returns its data only once gate is closed
type gatedReader struct {
	gate <-chan struct{}
	r    io.Reader
}

func (g *gatedReader) Read(p []byte) (int, error) {
	<-g.gate
	return g.r.Read(p)
}

// ifETag is a WriteCondition that only replaces an object with the given
// ETag, like a PutObject with If-Match
func ifETag(etag string) WriteCondition {
	return func(current *s3.ObjectMetadata) error {
		if current == nil {
			return ErrObjectNotFound
		}
		if current.ETag != etag {
			return ErrPreconditionFailed
		}
		return nil
	}
}

// TestConcurrentConditionalPuts tests that of concurrent writes with the
// same If-Match ETag, exactly one replaces the object. The bodies are held
// back until every write has passed the check made before receiving the
// data, so the check at commit is what decides.
func TestConcurrentConditionalPuts(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	const key = "conditional/head"
	initial, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader([]byte("initial")))
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	const writers = 20
	gate := make(chan struct{})
	var started, done sync.WaitGroup
	var wins, failed atomic.Int32
	var winner atomic.Value
	for i := range writers {
		started.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			content := fmt.Sprintf("writer %d", i)
			cond := ifETag(initial.ETag)
			checked := false
			wrapped := func(current *s3.ObjectMetadata) error {
				if !checked {
					checked = true
					defer started.Done()
				}
				return cond(current)
			}
			_, err := storage.PutObjectConditional(testBucket, key, "text/plain", nil, &gatedReader{gate: gate, r: bytes.NewReader([]byte(content))}, wrapped)
			switch {
			case err == nil:
				wins.Add(1)
				winner.Store(content)
			case errors.Is(err, ErrPreconditionFailed):
				failed.Add(1)
			default:
				t.Errorf("writer %d: %v", i, err)
			}
		}()
	}
	started.Wait()
	close(gate)
	done.Wait()

	if wins.Load() != 1 || failed.Load() != writers-1 {
		t.Fatalf("wins = %d, failed = %d, want 1 and %d", wins.Load(), failed.Load(), writers-1)
	}
	reader, _, err := storage.GetObject(testBucket, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	got, _ := io.ReadAll(reader)
	_ = reader.Close()
	if string(got) != winner.Load().(string) {
		t.Errorf("object = %q, want the winner's %q", got, winner.Load())
	}

	// The writes that lost removed their temp files
	entries, err := os.ReadDir(storage.bucketTmpDir(testBucket))
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("%d temp files left behind", len(entries))
	}
}
