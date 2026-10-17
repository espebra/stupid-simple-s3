package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/espebra/btree"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// indexDegree is the B-tree degree of bucket indexes
const indexDegree = 32

// indexEntry holds what a listing returns for one object
type indexEntry struct {
	key          string
	size         int64
	etag         string
	lastModified time.Time
}

func newIndexEntry(meta *s3.ObjectMetadata) *indexEntry {
	return &indexEntry{key: meta.Key, size: meta.Size, etag: meta.ETag, lastModified: meta.LastModified}
}

func (e *indexEntry) metadata() s3.ObjectMetadata {
	return s3.ObjectMetadata{Key: e.key, Size: e.size, ETag: e.etag, LastModified: e.lastModified}
}

func indexEntryLess(a, b *indexEntry) bool {
	return a.key < b.key
}

// bucketIndex is an in-memory index of the objects in a bucket, sorted by
// key, so that listings need no disk access. Object directories are named
// by the hash of the key, so without it every listing has to read the
// meta.json of every object in the bucket.
//
// An index is built from disk on the first listing of a bucket and then
// kept up to date by the writes, which update it under the object's commit
// lock after the rename that makes the change visible.
type bucketIndex struct {
	mu   sync.RWMutex
	tree *btree.BTreeG[*indexEntry]
	// touched records the keys written or deleted while the index is being
	// built, so that the build does not overwrite them with what it read
	// from disk earlier. It is nil once the index is built.
	touched map[string]struct{}

	// built is closed when the build has finished, successfully or with err
	built chan struct{}
	err   error
}

func newBucketIndex() *bucketIndex {
	return &bucketIndex{
		tree:    btree.NewG(indexDegree, indexEntryLess),
		touched: make(map[string]struct{}),
		built:   make(chan struct{}),
	}
}

// set adds or replaces an object written by a client
func (idx *bucketIndex) set(meta *s3.ObjectMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tree.ReplaceOrInsert(newIndexEntry(meta))
	if idx.touched != nil {
		idx.touched[meta.Key] = struct{}{}
	}
}

// remove removes an object deleted by a client
func (idx *bucketIndex) remove(key string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tree.Delete(&indexEntry{key: key})
	if idx.touched != nil {
		idx.touched[key] = struct{}{}
	}
}

// addFromDisk adds an object found while building the index, unless a
// client has written or deleted it since the build started
func (idx *bucketIndex) addFromDisk(meta *s3.ObjectMetadata) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if _, ok := idx.touched[meta.Key]; !ok {
		idx.tree.ReplaceOrInsert(newIndexEntry(meta))
	}
}

// loadIndex returns the index of a bucket, building it from disk if this
// is the first listing of the bucket. Concurrent callers wait for the same
// build.
func (fs *FilesystemStorage) loadIndex(bucket string) (*bucketIndex, error) {
	fs.indexMu.Lock()
	if fs.indexes == nil {
		fs.indexes = make(map[string]*bucketIndex)
	}
	idx, ok := fs.indexes[bucket]
	if !ok {
		idx = newBucketIndex()
		fs.indexes[bucket] = idx
	}
	fs.indexMu.Unlock()

	if !ok {
		fs.buildIndex(bucket, idx)
	}
	<-idx.built
	if idx.err != nil {
		return nil, idx.err
	}
	return idx, nil
}

// existingIndex returns the index of a bucket if one exists or is being
// built, for writers to update
func (fs *FilesystemStorage) existingIndex(bucket string) *bucketIndex {
	fs.indexMu.Lock()
	defer fs.indexMu.Unlock()
	return fs.indexes[bucket]
}

// dropIndex forgets the index of a deleted bucket
func (fs *FilesystemStorage) dropIndex(bucket string) {
	fs.indexMu.Lock()
	defer fs.indexMu.Unlock()
	delete(fs.indexes, bucket)
}

// buildIndex reads the metadata of every object in the bucket into idx.
// The index is registered before the walk starts, so a write that commits
// during the walk updates it, and one that committed before is seen by the
// walk.
func (fs *FilesystemStorage) buildIndex(bucket string, idx *bucketIndex) {
	objectsPath := filepath.Join(fs.basePath, "buckets", bucket, "objects")
	err := filepath.WalkDir(objectsPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if path == objectsPath {
				return err
			}
			// Skip directories removed or unreadable during the walk
			return nil
		}
		if !d.IsDir() || path == objectsPath {
			return nil
		}
		meta, err := readObjectMetadata(filepath.Join(path, "meta.json"))
		if err != nil {
			// A shard directory, or an object being written or deleted
			return nil
		}
		idx.addFromDisk(meta)
		return nil
	})

	idx.mu.Lock()
	idx.touched = nil
	idx.mu.Unlock()

	if err != nil {
		idx.err = fmt.Errorf("walking objects directory: %w", err)
		fs.indexMu.Lock()
		if fs.indexes[bucket] == idx {
			delete(fs.indexes, bucket)
		}
		fs.indexMu.Unlock()
	}
	close(idx.built)
}

// prefixSuccessor returns the smallest string greater than every string
// that starts with prefix, or false if there is none
func prefixSuccessor(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}
