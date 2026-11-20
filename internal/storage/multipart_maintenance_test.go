package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// mmUpload creates a multipart upload with one part per data string and
// returns its ID and the parts to complete it with
func mmUpload(t *testing.T, storage *FilesystemStorage, key string, data ...string) (string, []s3.CompletedPartInput) {
	t.Helper()
	uploadID, err := storage.CreateMultipartUpload(testBucket, key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	var parts []s3.CompletedPartInput
	for i, d := range data {
		meta, err := storage.UploadPart(uploadID, i+1, strings.NewReader(d))
		if err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
		parts = append(parts, s3.CompletedPartInput{PartNumber: i + 1, ETag: meta.ETag})
	}
	return uploadID, parts
}

func TestCountActiveUploads(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	assertCount := func(t *testing.T, want int) {
		t.Helper()
		if got := storage.CountActiveUploads(); got != want {
			t.Errorf("CountActiveUploads = %d, want %d", got, want)
		}
	}

	assertCount(t, 0)

	completed, completedParts := mmUpload(t, storage, "completed", "data")
	assertCount(t, 1)
	aborted, _ := mmUpload(t, storage, "aborted", "data")
	assertCount(t, 2)
	mmUpload(t, storage, "kept")
	assertCount(t, 3)

	if _, err := storage.CompleteMultipartUpload(completed, completedParts); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}
	assertCount(t, 2)
	if err := storage.AbortMultipartUpload(aborted); err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}
	assertCount(t, 1)

	// Failed operations do not change the count
	if _, err := storage.CompleteMultipartUpload(completed, completedParts); err == nil {
		t.Error("completing a completed upload succeeded")
	}
	if err := storage.AbortMultipartUpload(aborted); err == nil {
		t.Error("aborting an aborted upload succeeded")
	}
	if _, err := storage.CreateMultipartUpload(testBucket, strings.Repeat("k", 1025), "text/plain", nil); err == nil {
		t.Error("creating an upload with an invalid key succeeded")
	}
	assertCount(t, 1)

	// Files in the multipart directory are not uploads
	if err := os.WriteFile(filepath.Join(storage.multipartPath, "stray-file"), []byte("x"), 0600); err != nil {
		t.Fatalf("writing stray file: %v", err)
	}
	assertCount(t, 1)

	if err := os.RemoveAll(storage.multipartPath); err != nil {
		t.Fatalf("removing multipart directory: %v", err)
	}
	assertCount(t, 0)
}

// mmSetDirTime sets the modification time of an upload directory, after
// anything written into it
func mmSetDirTime(t *testing.T, dir string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(dir, mtime, mtime); err != nil {
		t.Fatalf("Chtimes: %v", err)
	}
}

func TestCleanupStaleUploadsUnreadableMetadata(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	old := time.Now().Add(-2 * time.Hour)
	recent := time.Now().Add(-time.Minute)

	// Upload directories that CreateMultipartUpload could leave behind if the
	// process stopped while writing the metadata, or that were damaged later
	type dirCase struct {
		name     string
		meta     *string // nil for no meta.json
		mtime    time.Time
		wantGone bool
	}
	corrupt := `{"uploadId": "trunc`
	empty := ""
	cases := []dirCase{
		{"corrupt metadata, old", &corrupt, old, true},
		{"corrupt metadata, recent", &corrupt, recent, false},
		{"empty metadata, old", &empty, old, true},
		{"missing metadata, old", nil, old, true},
		{"missing metadata, recent", nil, recent, false},
	}
	dirs := make([]string, len(cases))
	for i, tc := range cases {
		dir := filepath.Join(storage.multipartPath, uuid.New().String())
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatalf("%s: Mkdir: %v", tc.name, err)
		}
		if tc.meta != nil {
			if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(*tc.meta), 0600); err != nil {
				t.Fatalf("%s: writing meta.json: %v", tc.name, err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "part.00001"), []byte("data"), 0600); err != nil {
			t.Fatalf("%s: writing part: %v", tc.name, err)
		}
		mmSetDirTime(t, dir, tc.mtime)
		dirs[i] = dir
	}

	// Valid uploads are judged by their creation time, not the directory's
	// modification time
	staleID, _ := mmUpload(t, storage, "stale", "data")
	staleMetaPath := filepath.Join(storage.uploadDir(staleID), "meta.json")
	var staleMeta s3.MultipartUploadMetadata
	if err := readJSONFile(staleMetaPath, &staleMeta); err != nil {
		t.Fatalf("reading upload metadata: %v", err)
	}
	staleMeta.Created = old.UTC()
	if err := os.Remove(staleMetaPath); err != nil {
		t.Fatalf("removing upload metadata: %v", err)
	}
	if err := writeJSONFile(staleMetaPath, &staleMeta); err != nil {
		t.Fatalf("writing upload metadata: %v", err)
	}
	// A recent directory time does not keep a stale upload
	mmSetDirTime(t, storage.uploadDir(staleID), recent)

	freshID, _ := mmUpload(t, storage, "fresh", "data")
	// An old directory time does not remove a fresh upload
	mmSetDirTime(t, storage.uploadDir(freshID), old)

	cleaned, err := storage.CleanupStaleUploads(time.Hour)
	if err != nil {
		t.Fatalf("CleanupStaleUploads: %v", err)
	}

	wantCleaned := 1 // the stale upload
	for i, tc := range cases {
		_, statErr := os.Stat(dirs[i])
		gone := os.IsNotExist(statErr)
		if gone != tc.wantGone {
			t.Errorf("%s: removed = %v, want %v", tc.name, gone, tc.wantGone)
		}
		if tc.wantGone {
			wantCleaned++
		}
	}
	if cleaned != wantCleaned {
		t.Errorf("cleaned = %d, want %d", cleaned, wantCleaned)
	}
	if _, err := storage.GetMultipartUpload(staleID); !errors.Is(err, ErrUploadNotFound) {
		t.Errorf("stale upload: err = %v, want ErrUploadNotFound", err)
	}
	if _, err := storage.GetMultipartUpload(freshID); err != nil {
		t.Errorf("fresh upload was removed: %v", err)
	}
	if got := storage.CountActiveUploads(); got != 3 {
		t.Errorf("CountActiveUploads = %d, want 3 (two recent damaged directories and the fresh upload)", got)
	}
}

// TestCleanupStaleUploadsDuringComplete tests that an upload completed while
// the cleanup runs is either completed or cleaned up, never both
func TestCleanupStaleUploadsDuringComplete(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	for i := 0; i < 20; i++ {
		uploadID, parts := mmUpload(t, storage, "racing", "data")

		var wg sync.WaitGroup
		var completeErr error
		var cleaned int
		var cleanupErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, completeErr = storage.CompleteMultipartUpload(uploadID, parts)
		}()
		go func() {
			defer wg.Done()
			cleaned, cleanupErr = storage.CleanupStaleUploads(0)
		}()
		wg.Wait()

		if cleanupErr != nil {
			t.Fatalf("CleanupStaleUploads: %v", cleanupErr)
		}
		completed := 0
		if completeErr == nil {
			completed = 1
		} else if !errors.Is(completeErr, ErrUploadNotFound) {
			t.Fatalf("CompleteMultipartUpload: %v", completeErr)
		}
		if completed+cleaned != 1 {
			t.Fatalf("iteration %d: completed = %d, cleaned = %d, want exactly one", i, completed, cleaned)
		}
		if exists, _ := storage.ObjectExists(testBucket, "racing"); exists != (completed == 1) {
			t.Fatalf("iteration %d: object exists = %v after completed = %d", i, exists, completed)
		}
		if got := storage.CountActiveUploads(); got != 0 {
			t.Fatalf("iteration %d: CountActiveUploads = %d, want 0", i, got)
		}
		_ = storage.DeleteObject(testBucket, "racing")
	}
}

func TestCompleteMultipartUploadFailureCleanup(t *testing.T) {
	assertNothingLeft := func(t *testing.T, storage *FilesystemStorage, uploadID, key string) {
		t.Helper()
		if entries := tmpDirEntries(t, storage); len(entries) != 0 {
			t.Errorf("temp directory has %d entries after the failed completion, want 0", len(entries))
		}
		if exists, _ := storage.ObjectExists(testBucket, key); exists {
			t.Error("failed completion created the object")
		}
		if entries := objectsDirEntries(t, storage); len(entries) != 0 {
			t.Errorf("objects directory has %d entries after the failed completion, want 0", len(entries))
		}
		// The upload is kept, so the client can retry or abort it
		if _, err := storage.GetMultipartUpload(uploadID); err != nil {
			t.Errorf("upload is gone after the failed completion: %v", err)
		}
	}

	t.Run("part file missing", func(t *testing.T) {
		storage, cleanup := setupTestStorage(t)
		defer cleanup()
		uploadID, parts := mmUpload(t, storage, "obj", "first", "second")
		if err := os.Remove(filepath.Join(storage.uploadDir(uploadID), "part.00002")); err != nil {
			t.Fatalf("removing part: %v", err)
		}
		if _, err := storage.CompleteMultipartUpload(uploadID, parts); !errors.Is(err, ErrPartNotFound) {
			t.Errorf("err = %v, want ErrPartNotFound", err)
		}
		assertNothingLeft(t, storage, uploadID, "obj")
	})

	t.Run("part metadata corrupt", func(t *testing.T) {
		storage, cleanup := setupTestStorage(t)
		defer cleanup()
		uploadID, parts := mmUpload(t, storage, "obj", "first", "second")
		if err := os.WriteFile(filepath.Join(storage.uploadDir(uploadID), "part.00002.meta"), []byte("{"), 0600); err != nil {
			t.Fatalf("writing part metadata: %v", err)
		}
		if _, err := storage.CompleteMultipartUpload(uploadID, parts); err == nil {
			t.Error("CompleteMultipartUpload succeeded with corrupt part metadata")
		}
		assertNothingLeft(t, storage, uploadID, "obj")
	})

	t.Run("part replaced with a directory", func(t *testing.T) {
		storage, cleanup := setupTestStorage(t)
		defer cleanup()
		uploadID, parts := mmUpload(t, storage, "obj", "first", "second")
		partPath := filepath.Join(storage.uploadDir(uploadID), "part.00002")
		if err := os.Remove(partPath); err != nil {
			t.Fatalf("removing part: %v", err)
		}
		if err := os.Mkdir(partPath, 0700); err != nil {
			t.Fatalf("creating directory: %v", err)
		}
		if _, err := storage.CompleteMultipartUpload(uploadID, parts); err == nil {
			t.Error("CompleteMultipartUpload succeeded with a directory for a part")
		}
		assertNothingLeft(t, storage, uploadID, "obj")
	})

	// An unreadable part passes the checks and fails while the parts are
	// copied into the temp file, after the first part has been copied
	t.Run("part unreadable", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can read files without read permission")
		}
		storage, cleanup := setupTestStorage(t)
		defer cleanup()
		uploadID, parts := mmUpload(t, storage, "obj", "first", "second")
		partPath := filepath.Join(storage.uploadDir(uploadID), "part.00002")
		if err := os.Chmod(partPath, 0); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		defer func() { _ = os.Chmod(partPath, 0600) }()

		if _, err := storage.CompleteMultipartUpload(uploadID, parts); err == nil {
			t.Fatal("CompleteMultipartUpload succeeded with an unreadable part")
		}
		assertNothingLeft(t, storage, uploadID, "obj")

		// Once the part is readable again, the completion can be retried
		if err := os.Chmod(partPath, 0600); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		if _, err := storage.CompleteMultipartUpload(uploadID, parts); err != nil {
			t.Fatalf("retried CompleteMultipartUpload: %v", err)
		}
		if entries := tmpDirEntries(t, storage); len(entries) != 0 {
			t.Errorf("temp directory has %d entries after the retried completion, want 0", len(entries))
		}
	})
}
