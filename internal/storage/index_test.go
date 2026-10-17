package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// listAll lists every object and common prefix in the bucket, page by page
func listAll(t *testing.T, storage *FilesystemStorage, opts ListObjectsOptions) []string {
	t.Helper()
	var got []string
	for range 10000 {
		result, err := storage.ListObjects(testBucket, opts)
		if err != nil {
			t.Fatalf("ListObjects failed: %v", err)
		}
		for _, obj := range result.Objects {
			got = append(got, fmt.Sprintf("%s %d %s", obj.Key, obj.Size, obj.ETag))
		}
		got = append(got, result.CommonPrefixes...)
		if !result.IsTruncated {
			return got
		}
		opts.ContinuationToken = result.NextContinuationToken
	}
	t.Fatal("listing did not finish")
	return nil
}

// reopen returns a new storage over the same directories, whose index is
// built from disk on its first listing
func reopen(t *testing.T, storage *FilesystemStorage) *FilesystemStorage {
	t.Helper()
	fresh, err := NewFilesystemStorage(storage.basePath, storage.multipartPath)
	if err != nil {
		t.Fatalf("NewFilesystemStorage failed: %v", err)
	}
	return fresh
}

func put(t *testing.T, storage *FilesystemStorage, key, content string) {
	t.Helper()
	if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader([]byte(content))); err != nil {
		t.Fatalf("PutObject(%q) failed: %v", key, err)
	}
}

func TestIndexFollowsWrites(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	put(t, storage, "a", "one")
	put(t, storage, "b", "two")
	listAll(t, storage, ListObjectsOptions{}) // builds the index

	put(t, storage, "c", "three")
	put(t, storage, "a", "overwritten with a longer value")
	if err := storage.DeleteObject(testBucket, "b"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if _, err := storage.CopyObject(testBucket, "c", testBucket, "d"); err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}
	uploadID, err := storage.CreateMultipartUpload(testBucket, "e", "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	part, err := storage.UploadPart(uploadID, 1, bytes.NewReader([]byte("multipart")))
	if err != nil {
		t.Fatalf("UploadPart failed: %v", err)
	}
	if _, err := storage.CompleteMultipartUpload(uploadID, []s3.CompletedPartInput{{PartNumber: 1, ETag: part.ETag}}); err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	got := listAll(t, storage, ListObjectsOptions{})
	want := listAll(t, reopen(t, storage), ListObjectsOptions{})
	if !slices.Equal(got, want) {
		t.Errorf("index listing = %v\nlisting from disk = %v", got, want)
	}
	if len(got) != 4 {
		t.Errorf("listed %d objects, want 4 (a, c, d, e): %v", len(got), got)
	}
}

// TestIndexBuildDuringWrites tests that writes and deletes that happen while
// the index is being built end up in it, and that the build does not bring
// back stale or deleted objects
func TestIndexBuildDuringWrites(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	for i := range 500 {
		put(t, storage, fmt.Sprintf("key-%03d", i), "original")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 500 {
			key := fmt.Sprintf("key-%03d", i)
			var err error
			switch i % 3 {
			case 0:
				_, err = storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader([]byte("overwritten during build")))
			case 1:
				err = storage.DeleteObject(testBucket, key)
			case 2:
				_, err = storage.PutObject(testBucket, "new-"+key, "text/plain", nil, bytes.NewReader([]byte("new")))
			}
			if err != nil {
				t.Errorf("write %d failed: %v", i, err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := storage.ListObjects(testBucket, ListObjectsOptions{}); err != nil {
			t.Errorf("ListObjects failed: %v", err)
		}
	}()
	wg.Wait()

	got := listAll(t, storage, ListObjectsOptions{})
	want := listAll(t, reopen(t, storage), ListObjectsOptions{})
	if !slices.Equal(got, want) {
		t.Errorf("index differs from disk after writes during the build:\nindex: %d entries\ndisk:  %d entries", len(got), len(want))
	}
}

// TestListingDoesNotReadMetadata tests that once the index is built,
// listings are served from memory
func TestListingDoesNotReadMetadata(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	put(t, storage, "dir/a", "one")
	put(t, storage, "dir/b", "two")
	before := listAll(t, storage, ListObjectsOptions{})

	// Make every meta.json unreadable; a listing that read them would fail
	err := filepath.WalkDir(storage.basePath, func(path string, d os.DirEntry, err error) error {
		if err == nil && d.Name() == "meta.json" {
			return os.WriteFile(path, []byte("not json"), 0600)
		}
		return err
	})
	if err != nil {
		t.Fatalf("corrupting metadata: %v", err)
	}

	if after := listAll(t, storage, ListObjectsOptions{}); !slices.Equal(after, before) {
		t.Errorf("listing after the build = %v, want %v", after, before)
	}
}

func TestIndexDroppedWithBucket(t *testing.T) {
	storage, cleanup := setupTestStorage(t)
	defer cleanup()

	put(t, storage, "key", "data")
	listAll(t, storage, ListObjectsOptions{})
	if err := storage.DeleteObject(testBucket, "key"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if err := storage.DeleteBucket(testBucket); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if _, err := storage.ListObjects(testBucket, ListObjectsOptions{}); err == nil {
		t.Error("ListObjects of a deleted bucket succeeded")
	}
	if err := storage.CreateBucket(testBucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if got := listAll(t, storage, ListObjectsOptions{}); len(got) != 0 {
		t.Errorf("recreated bucket lists %v, want nothing", got)
	}
}

func TestPrefixSuccessor(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
		ok     bool
	}{
		{"a/", "a0", true},
		{"abc", "abd", true},
		{"a\xff", "b", true},
		{"\xff\xff", "", false},
		{"", "", false},
	}
	for _, tt := range tests {
		got, ok := prefixSuccessor(tt.prefix)
		if got != tt.want || ok != tt.ok {
			t.Errorf("prefixSuccessor(%q) = %q, %v; want %q, %v", tt.prefix, got, ok, tt.want, tt.ok)
		}
	}
}

// BenchmarkListObjects measures one page of a listing of a bucket with
// 10,000 objects, after the first listing
func BenchmarkListObjects(b *testing.B) {
	tmpDir := b.TempDir()
	storage, err := NewFilesystemStorage(filepath.Join(tmpDir, "data"), filepath.Join(tmpDir, "multipart"))
	if err != nil {
		b.Fatalf("NewFilesystemStorage failed: %v", err)
	}
	if err := storage.CreateBucket(testBucket); err != nil {
		b.Fatalf("CreateBucket failed: %v", err)
	}
	for i := range 10000 {
		key := fmt.Sprintf("dir-%02d/object-%05d", i%20, i)
		if _, err := storage.PutObject(testBucket, key, "text/plain", nil, bytes.NewReader([]byte("x"))); err != nil {
			b.Fatalf("PutObject failed: %v", err)
		}
	}
	if _, err := storage.ListObjects(testBucket, ListObjectsOptions{}); err != nil {
		b.Fatalf("ListObjects failed: %v", err)
	}

	for _, opts := range []ListObjectsOptions{{}, {Delimiter: "/"}, {Prefix: "dir-07/", MaxKeys: 100}} {
		b.Run(fmt.Sprintf("prefix=%q,delimiter=%q", opts.Prefix, opts.Delimiter), func(b *testing.B) {
			for b.Loop() {
				if _, err := storage.ListObjects(testBucket, opts); err != nil {
					b.Fatalf("ListObjects failed: %v", err)
				}
			}
		})
	}
}
