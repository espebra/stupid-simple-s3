package storage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// FilesystemStorage implements Storage using the local filesystem
type FilesystemStorage struct {
	basePath      string
	multipartPath string
}

// NewFilesystemStorage creates a new filesystem-backed storage
func NewFilesystemStorage(basePath, multipartPath string) (*FilesystemStorage, error) {
	// Create base directories if they don't exist
	objectsPath := filepath.Join(basePath, "objects")
	if _, err := os.Stat(objectsPath); os.IsNotExist(err) {
		if err := os.MkdirAll(objectsPath, 0755); err != nil {
			return nil, fmt.Errorf("creating objects directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("checking objects directory: %w", err)
	}

	if _, err := os.Stat(multipartPath); os.IsNotExist(err) {
		if err := os.MkdirAll(multipartPath, 0755); err != nil {
			return nil, fmt.Errorf("creating multipart directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("checking multipart directory: %w", err)
	}

	return &FilesystemStorage{
		basePath:      basePath,
		multipartPath: multipartPath,
	}, nil
}

// keyToPath converts an object key to a filesystem path
// Uses a 4-character hash prefix for directory distribution (65,536 buckets) and base64-encoded key
func (fs *FilesystemStorage) keyToPath(key string) string {
	// Create a hash prefix from the first 4 chars of MD5 of the key
	hash := md5.Sum([]byte(key))
	prefix := hex.EncodeToString(hash[:2])

	// Base64 encode the key for safe filesystem storage
	encodedKey := base64.URLEncoding.EncodeToString([]byte(key))

	return filepath.Join(fs.basePath, "objects", prefix, encodedKey)
}

// PutObject stores an object with the given key
func (fs *FilesystemStorage) PutObject(key string, contentType string, metadata map[string]string, body io.Reader) (*s3.ObjectMetadata, error) {
	objPath := fs.keyToPath(key)
	dataPath := filepath.Join(objPath, "data")
	metaPath := filepath.Join(objPath, "meta.json")

	// Create object directory
	if err := os.MkdirAll(objPath, 0755); err != nil {
		return nil, fmt.Errorf("creating object directory: %w", err)
	}

	// Write data to a temp file first, then rename
	tmpPath := dataPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	// Calculate MD5 while writing
	hash := md5.New()
	writer := io.MultiWriter(tmpFile, hash)

	size, err := io.Copy(writer, body)
	if err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return nil, fmt.Errorf("writing object data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("closing temp file: %w", err)
	}

	// Rename temp file to final location
	if err := os.Rename(tmpPath, dataPath); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("renaming temp file: %w", err)
	}

	// Create metadata
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))
	now := time.Now().UTC()

	objMeta := &s3.ObjectMetadata{
		Key:          key,
		Size:         size,
		ContentType:  contentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: metadata,
	}

	// Write metadata
	metaFile, err := os.Create(metaPath)
	if err != nil {
		return nil, fmt.Errorf("creating metadata file: %w", err)
	}
	defer metaFile.Close()

	if err := json.NewEncoder(metaFile).Encode(objMeta); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	return objMeta, nil
}

// GetObject retrieves an object by key
func (fs *FilesystemStorage) GetObject(key string) (io.ReadCloser, *s3.ObjectMetadata, error) {
	objPath := fs.keyToPath(key)
	dataPath := filepath.Join(objPath, "data")

	// Get metadata first
	meta, err := fs.HeadObject(key)
	if err != nil {
		return nil, nil, err
	}

	// Open data file
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("object not found: %s", key)
		}
		return nil, nil, fmt.Errorf("opening object data: %w", err)
	}

	return file, meta, nil
}

// HeadObject retrieves object metadata without the body
func (fs *FilesystemStorage) HeadObject(key string) (*s3.ObjectMetadata, error) {
	objPath := fs.keyToPath(key)
	metaPath := filepath.Join(objPath, "meta.json")

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found: %s", key)
		}
		return nil, fmt.Errorf("opening metadata file: %w", err)
	}
	defer metaFile.Close()

	var meta s3.ObjectMetadata
	if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return nil, fmt.Errorf("parsing metadata: %w", err)
	}

	return &meta, nil
}

// DeleteObject removes an object by key
func (fs *FilesystemStorage) DeleteObject(key string) error {
	objPath := fs.keyToPath(key)

	// Remove the entire object directory
	err := os.RemoveAll(objPath)
	if err != nil {
		return fmt.Errorf("removing object: %w", err)
	}

	// Try to remove empty parent directory (ignore errors)
	parentDir := filepath.Dir(objPath)
	os.Remove(parentDir) // only succeeds if empty

	return nil
}

// ObjectExists checks if an object exists
func (fs *FilesystemStorage) ObjectExists(key string) (bool, error) {
	objPath := fs.keyToPath(key)
	metaPath := filepath.Join(objPath, "meta.json")

	_, err := os.Stat(metaPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking object existence: %w", err)
}

// GetObjectRange retrieves a range of bytes from an object
func (fs *FilesystemStorage) GetObjectRange(key string, start, end int64) (io.ReadCloser, *s3.ObjectMetadata, error) {
	objPath := fs.keyToPath(key)
	dataPath := filepath.Join(objPath, "data")

	// Get metadata first
	meta, err := fs.HeadObject(key)
	if err != nil {
		return nil, nil, err
	}

	// Validate range
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= meta.Size {
		end = meta.Size - 1
	}
	if start > end {
		return nil, nil, fmt.Errorf("invalid range: start > end")
	}

	// Open data file
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, fmt.Errorf("object not found: %s", key)
		}
		return nil, nil, fmt.Errorf("opening object data: %w", err)
	}

	// Seek to start position
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("seeking to range start: %w", err)
	}

	// Return a limited reader that only reads the requested range
	rangeLength := end - start + 1
	limitedReader := &limitedReadCloser{
		reader: io.LimitReader(file, rangeLength),
		closer: file,
	}

	return limitedReader, meta, nil
}

// limitedReadCloser wraps a limited reader with a closer
type limitedReadCloser struct {
	reader io.Reader
	closer io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (n int, err error) {
	return l.reader.Read(p)
}

func (l *limitedReadCloser) Close() error {
	return l.closer.Close()
}

// ListObjects lists objects with optional prefix, delimiter, and pagination
func (fs *FilesystemStorage) ListObjects(opts ListObjectsOptions) (*ListObjectsResult, error) {
	if opts.MaxKeys <= 0 {
		opts.MaxKeys = 1000
	}
	if opts.MaxKeys > 1000 {
		opts.MaxKeys = 1000
	}

	objectsPath := filepath.Join(fs.basePath, "objects")
	var allObjects []s3.ObjectMetadata

	// Walk through all hash prefix directories
	err := filepath.WalkDir(objectsPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip non-directories and the root
		if !d.IsDir() {
			return nil
		}

		// Check if this is an object directory (contains meta.json)
		metaPath := filepath.Join(path, "meta.json")
		if _, statErr := os.Stat(metaPath); statErr != nil {
			return nil
		}

		// Read metadata
		metaFile, err := os.Open(metaPath)
		if err != nil {
			return nil
		}
		defer metaFile.Close()

		var meta s3.ObjectMetadata
		if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
			return nil
		}

		allObjects = append(allObjects, meta)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking objects directory: %w", err)
	}

	// Sort objects by key
	sortObjectsByKey(allObjects)

	// Apply filtering and pagination
	result := &ListObjectsResult{}
	commonPrefixSet := make(map[string]bool)
	startKey := opts.StartAfter
	if opts.ContinuationToken != "" {
		// Decode continuation token (it's base64 encoded key)
		decoded, err := base64.URLEncoding.DecodeString(opts.ContinuationToken)
		if err == nil {
			startKey = string(decoded)
		}
	}

	count := 0
	for _, obj := range allObjects {
		// Skip objects before startKey
		if startKey != "" && obj.Key <= startKey {
			continue
		}

		// Apply prefix filter
		if opts.Prefix != "" && !hasPrefix(obj.Key, opts.Prefix) {
			continue
		}

		// Handle delimiter (for common prefixes / virtual directories)
		if opts.Delimiter != "" {
			// Find delimiter after prefix
			afterPrefix := obj.Key[len(opts.Prefix):]
			delimIdx := indexOf(afterPrefix, opts.Delimiter)
			if delimIdx >= 0 {
				// This is a common prefix
				commonPrefix := opts.Prefix + afterPrefix[:delimIdx+len(opts.Delimiter)]
				if !commonPrefixSet[commonPrefix] {
					commonPrefixSet[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
				continue
			}
		}

		// Check if we've reached the limit
		if count >= opts.MaxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = base64.URLEncoding.EncodeToString([]byte(result.Objects[len(result.Objects)-1].Key))
			break
		}

		result.Objects = append(result.Objects, obj)
		count++
	}

	return result, nil
}

// CopyObject copies an object from source key to destination key
func (fs *FilesystemStorage) CopyObject(srcKey, dstKey string) (*s3.ObjectMetadata, error) {
	// Get source object
	srcReader, srcMeta, err := fs.GetObject(srcKey)
	if err != nil {
		return nil, err
	}
	defer srcReader.Close()

	// Copy to destination
	dstMeta, err := fs.PutObject(dstKey, srcMeta.ContentType, srcMeta.UserMetadata, srcReader)
	if err != nil {
		return nil, fmt.Errorf("copying object: %w", err)
	}

	return dstMeta, nil
}

// Helper functions

func sortObjectsByKey(objects []s3.ObjectMetadata) {
	// Simple insertion sort for stability
	for i := 1; i < len(objects); i++ {
		key := objects[i]
		j := i - 1
		for j >= 0 && objects[j].Key > key.Key {
			objects[j+1] = objects[j]
			j--
		}
		objects[j+1] = key
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
