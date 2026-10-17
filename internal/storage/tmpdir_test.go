package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// errAfter is a reader that fails after returning some data
type errAfter struct{ n int }

func (r *errAfter) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, errors.New("client went away")
	}
	n := min(len(p), r.n)
	r.n -= n
	return n, nil
}

func tmpDirEntries(t *testing.T, storage *FilesystemStorage) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(storage.bucketTmpDir(testBucket))
	if err != nil {
		t.Fatalf("reading temp directory: %v", err)
	}
	return entries
}

func TestWritesLeaveNoTempFiles(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	if _, err := storage.PutObject(testBucket, "ok", "text/plain", nil, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if _, err := storage.PutObject(testBucket, "failed", "text/plain", nil, &errAfter{n: 100}); err == nil {
		t.Fatal("PutObject with a failing body succeeded")
	}
	if entries := tmpDirEntries(t, storage); len(entries) != 0 {
		t.Errorf("temp directory has %d entries after the writes, want 0", len(entries))
	}
}

// TestTempDirCreatedForExistingBucket tests a bucket created before buckets
// had a temp directory
func TestTempDirCreatedForExistingBucket(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := os.Remove(storage.bucketTmpDir(testBucket)); err != nil {
		t.Fatalf("removing temp directory: %v", err)
	}
	if _, err := storage.PutObject(testBucket, "key", "text/plain", nil, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	reader, _, err := storage.GetObject(testBucket, "key")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer func() { _ = reader.Close() }()
	if data, _ := io.ReadAll(reader); string(data) != "data" {
		t.Errorf("object data = %q, want %q", data, "data")
	}
}

func TestCleanupStaleTempFiles(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	if _, err := storage.PutObject(testBucket, "key", "text/plain", nil, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	tmpDir := storage.bucketTmpDir(testBucket)
	stale := filepath.Join(tmpDir, "stale.data")
	fresh := filepath.Join(tmpDir, "fresh.data")
	for _, path := range []string{stale, fresh} {
		if err := os.WriteFile(path, []byte("leftover"), 0600); err != nil {
			t.Fatalf("writing %s: %v", path, err)
		}
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatalf("Chtimes failed: %v", err)
	}

	cleaned, err := storage.CleanupStaleTempFiles(time.Hour)
	if err != nil {
		t.Fatalf("CleanupStaleTempFiles failed: %v", err)
	}
	if cleaned != 1 {
		t.Errorf("cleaned %d files, want 1", cleaned)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Error("stale temp file was not removed")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("fresh temp file was removed: %v", err)
	}
	if exists, _ := storage.ObjectExists(testBucket, "key"); !exists {
		t.Error("cleanup removed an object")
	}
}

func TestDeleteBucketRemovesTempDir(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	if err := os.WriteFile(filepath.Join(storage.bucketTmpDir(testBucket), "leftover.data"), []byte("x"), 0600); err != nil {
		t.Fatalf("writing leftover temp file: %v", err)
	}
	if err := storage.DeleteBucket(testBucket); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(storage.basePath, "buckets", testBucket)); !os.IsNotExist(err) {
		t.Errorf("bucket directory still exists after DeleteBucket: %v", err)
	}
}
