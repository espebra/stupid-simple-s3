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

func objectsDirEntries(t *testing.T, storage *FilesystemStorage) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(storage.basePath, "buckets", testBucket, "objects"))
	if err != nil {
		t.Fatalf("reading objects directory: %v", err)
	}
	return entries
}

// TestFailedPutLeavesNoObjectDir tests that a failed upload does not leave
// an empty object directory, which would keep the bucket from being deleted
func TestFailedPutLeavesNoObjectDir(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	if _, err := storage.PutObject(testBucket, "failed", "text/plain", nil, &errAfter{n: 100}); err == nil {
		t.Fatal("PutObject with a failing body succeeded")
	}
	if entries := objectsDirEntries(t, storage); len(entries) != 0 {
		t.Errorf("objects directory has %d entries after a failed upload, want 0", len(entries))
	}
	if err := storage.DeleteBucket(testBucket); err != nil {
		t.Errorf("DeleteBucket after a failed upload: %v", err)
	}
}

// TestDeleteBucketRemovesLeftoverObjectDirs tests that empty object
// directories left by failed uploads of earlier versions do not keep the
// bucket from being deleted, while real objects do
func TestDeleteBucketRemovesLeftoverObjectDirs(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	leftover, err := storage.keyToPath(testBucket, "failed-upload")
	if err != nil {
		t.Fatalf("keyToPath failed: %v", err)
	}
	if err := os.MkdirAll(leftover, 0700); err != nil {
		t.Fatalf("creating leftover directory: %v", err)
	}

	t.Run("bucket with an object is not empty", func(t *testing.T) {
		if _, err := storage.PutObject(testBucket, "real", "text/plain", nil, bytes.NewReader([]byte("data"))); err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		if err := storage.DeleteBucket(testBucket); !errors.Is(err, ErrBucketNotEmpty) {
			t.Fatalf("DeleteBucket err = %v, want ErrBucketNotEmpty", err)
		}
		if exists, _ := storage.ObjectExists(testBucket, "real"); !exists {
			t.Error("object removed by a failed DeleteBucket")
		}
		if err := storage.DeleteObject(testBucket, "real"); err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}
	})

	t.Run("bucket with only leftovers is deleted", func(t *testing.T) {
		if err := os.MkdirAll(leftover, 0700); err != nil {
			t.Fatalf("creating leftover directory: %v", err)
		}
		if err := storage.DeleteBucket(testBucket); err != nil {
			t.Fatalf("DeleteBucket failed: %v", err)
		}
		if _, err := os.Stat(filepath.Join(storage.basePath, "buckets", testBucket)); !os.IsNotExist(err) {
			t.Errorf("bucket directory still exists: %v", err)
		}
	})
}

// TestUploadInProgressBlocksDeleteBucket tests that a bucket is not deleted
// under an upload that is still receiving data
func TestUploadInProgressBlocksDeleteBucket(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	body, writer := io.Pipe()
	done := make(chan error)
	go func() {
		_, err := storage.PutObject(testBucket, "slow", "text/plain", nil, body)
		done <- err
	}()
	if _, err := writer.Write([]byte("first part")); err != nil {
		t.Fatalf("writing to upload: %v", err)
	}

	if err := storage.DeleteBucket(testBucket); !errors.Is(err, ErrBucketNotEmpty) {
		t.Errorf("DeleteBucket during an upload: err = %v, want ErrBucketNotEmpty", err)
	}

	_ = writer.Close()
	if err := <-done; err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if exists, _ := storage.ObjectExists(testBucket, "slow"); !exists {
		t.Error("uploaded object does not exist")
	}
}

// TestWriteThroughPlantedSymlinkRejected tests that a symlink planted in the
// storage tree cannot make a write land outside the base path, at any level
// below it
func TestWriteThroughPlantedSymlinkRejected(t *testing.T) {
	for name, levels := range map[string]int{
		"object directory":  0,
		"shard directory":   1,
		"objects directory": 2,
	} {
		t.Run(name, func(t *testing.T) {
			storage, cleanup := setupTestStorage(t)
			defer cleanup()

			objPath, err := storage.keyToPath(testBucket, "redirected")
			if err != nil {
				t.Fatalf("keyToPath failed: %v", err)
			}
			planted := objPath
			for range levels {
				planted = filepath.Dir(planted)
			}
			// The symlink replaces an existing directory, and points to an
			// empty one outside the base path
			_ = os.RemoveAll(planted)
			if err := os.MkdirAll(filepath.Dir(planted), 0700); err != nil {
				t.Fatalf("creating parent of symlink: %v", err)
			}
			outside := t.TempDir()
			if err := os.Symlink(outside, planted); err != nil {
				t.Fatalf("planting symlink: %v", err)
			}
			// The shard directory must exist below an objects directory
			// symlink for the write to get as far as the check
			if levels == 2 {
				_ = os.Mkdir(filepath.Join(outside, filepath.Base(filepath.Dir(objPath))), 0700)
			}

			_, err = storage.PutObject(testBucket, "redirected", "text/plain", nil, bytes.NewReader([]byte("data")))
			if !errors.Is(err, ErrInvalidKey) {
				t.Errorf("PutObject through a planted symlink: err = %v, want ErrInvalidKey", err)
			}
			_ = filepath.WalkDir(outside, func(path string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					t.Errorf("write left %s outside the base path", path)
				}
				return nil
			})
		})
	}
}

// TestWriteThroughSymlinkInsideBase tests that a symlink in the storage tree
// that resolves to a directory inside the base path is followed
func TestWriteThroughSymlinkInsideBase(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	objPath, err := storage.keyToPath(testBucket, "moved")
	if err != nil {
		t.Fatalf("keyToPath failed: %v", err)
	}
	shard := filepath.Dir(objPath)
	target := filepath.Join(storage.basePath, "moved-shard")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatalf("creating target: %v", err)
	}
	if err := os.Symlink(target, shard); err != nil {
		t.Fatalf("planting symlink: %v", err)
	}

	if _, err := storage.PutObject(testBucket, "moved", "text/plain", nil, bytes.NewReader([]byte("data"))); err != nil {
		t.Fatalf("PutObject through a symlink inside the base path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(target, filepath.Base(objPath), "data")); err != nil {
		t.Errorf("object not written through the symlink: %v", err)
	}
}
