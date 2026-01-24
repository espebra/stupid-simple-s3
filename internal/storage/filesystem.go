package storage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// ErrInvalidKey is returned when an object key fails validation
var ErrInvalidKey = errors.New("invalid object key")

// ErrInvalidBucketName is returned when a bucket name fails validation
var ErrInvalidBucketName = errors.New("invalid bucket name")

// ErrBucketNotFound is returned when a bucket does not exist
var ErrBucketNotFound = errors.New("bucket not found")

// ErrBucketAlreadyExists is returned when trying to create a bucket that already exists
var ErrBucketAlreadyExists = errors.New("bucket already exists")

// ErrBucketNotEmpty is returned when trying to delete a non-empty bucket
var ErrBucketNotEmpty = errors.New("bucket not empty")

// ErrObjectNotFound is returned when an object does not exist
var ErrObjectNotFound = errors.New("object not found")

// ErrEntityTooLarge is returned when an object or part exceeds size limits
var ErrEntityTooLarge = errors.New("entity too large")

// ErrUploadNotFound is returned when a multipart upload does not exist
var ErrUploadNotFound = errors.New("upload not found")

// ErrPartNotFound is returned when a multipart upload part does not exist
var ErrPartNotFound = errors.New("part not found")

// ErrInvalidPartOrder is returned when parts are not in ascending order
var ErrInvalidPartOrder = errors.New("parts must be in ascending order")

// ValidateKey checks that an object key is safe and doesn't contain path traversal sequences.
// Returns an error if the key is invalid.
func ValidateKey(key string) error {
	// Reject empty keys
	if key == "" {
		return fmt.Errorf("%w: key cannot be empty", ErrInvalidKey)
	}

	// Reject keys containing null bytes
	if strings.ContainsRune(key, '\x00') {
		return fmt.Errorf("%w: key cannot contain null bytes", ErrInvalidKey)
	}

	// Reject absolute paths
	if strings.HasPrefix(key, "/") {
		return fmt.Errorf("%w: key cannot be an absolute path", ErrInvalidKey)
	}

	// Normalize backslashes to forward slashes for checking
	// (on Unix, backslash is a valid filename char, but we reject it for safety)
	normalizedKey := strings.ReplaceAll(key, "\\", "/")

	// Check for ".." as a path component in the original key
	// This catches traversal attempts before filepath.Clean normalizes them away
	parts := strings.Split(normalizedKey, "/")
	for _, part := range parts {
		if part == ".." {
			return fmt.Errorf("%w: key cannot contain path traversal sequences", ErrInvalidKey)
		}
	}

	// Also verify the cleaned path doesn't escape (defense in depth)
	cleaned := filepath.Clean(key)
	if filepath.IsAbs(cleaned) {
		return fmt.Errorf("%w: key cannot be an absolute path", ErrInvalidKey)
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: key cannot traverse above root", ErrInvalidKey)
	}

	return nil
}

// ValidateBucketName checks that a bucket name follows S3 naming rules.
// Rules: 3-63 characters, lowercase letters, numbers, and hyphens only.
// Must start and end with a letter or number.
func ValidateBucketName(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("%w: bucket name must be between 3 and 63 characters", ErrInvalidBucketName)
	}

	for i, c := range name {
		isLower := c >= 'a' && c <= 'z'
		isDigit := c >= '0' && c <= '9'
		isHyphen := c == '-'

		if !isLower && !isDigit && !isHyphen {
			return fmt.Errorf("%w: bucket name can only contain lowercase letters, numbers, and hyphens", ErrInvalidBucketName)
		}

		// First and last character must be letter or number
		if (i == 0 || i == len(name)-1) && isHyphen {
			return fmt.Errorf("%w: bucket name must start and end with a letter or number", ErrInvalidBucketName)
		}
	}

	return nil
}

// FilesystemStorage implements Storage using the local filesystem
type FilesystemStorage struct {
	basePath      string
	multipartPath string
}

// NewFilesystemStorage creates a new filesystem-backed storage
func NewFilesystemStorage(basePath, multipartPath string) (*FilesystemStorage, error) {
	// Create base directories if they don't exist
	bucketsPath := filepath.Join(basePath, "buckets")
	if _, err := os.Stat(bucketsPath); os.IsNotExist(err) {
		if err := os.MkdirAll(bucketsPath, 0700); err != nil {
			return nil, fmt.Errorf("creating buckets directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("checking buckets directory: %w", err)
	}

	if _, err := os.Stat(multipartPath); os.IsNotExist(err) {
		if err := os.MkdirAll(multipartPath, 0700); err != nil {
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

// keyToPath converts an object key to a filesystem path within a bucket
// Uses a 4-character hash prefix for directory distribution (65,536 directories) and base64-encoded key
// Returns an error if the key is invalid or the resulting path would escape the base directory.
func (fs *FilesystemStorage) keyToPath(bucket, key string) (string, error) {
	// Validate the bucket name
	if err := ValidateBucketName(bucket); err != nil {
		return "", err
	}

	// Validate the key
	if err := ValidateKey(key); err != nil {
		return "", err
	}

	// Create a hash prefix from the first 4 chars of MD5 of the key
	hash := md5.Sum([]byte(key))
	prefix := hex.EncodeToString(hash[:2])

	// Base64 encode the key for safe filesystem storage
	encodedKey := base64.URLEncoding.EncodeToString([]byte(key))

	result := filepath.Join(fs.basePath, "buckets", bucket, "objects", prefix, encodedKey)

	// Defense in depth: verify the resulting path is within basePath
	// First, resolve the base path (which should always exist)
	absBase, err := filepath.Abs(fs.basePath)
	if err != nil {
		return "", fmt.Errorf("%w: failed to resolve base path", ErrInvalidKey)
	}
	// Resolve symlinks on base path to handle cases like /tmp -> /private/tmp on macOS
	if realBase, err := filepath.EvalSymlinks(absBase); err == nil {
		absBase = realBase
	}

	// For the result path, we need to resolve what exists and verify the rest
	// Since the full path may not exist yet, resolve from the base and append the relative part
	relPath := filepath.Join("buckets", bucket, "objects", prefix, encodedKey)
	absResult := filepath.Join(absBase, relPath)

	// If the result path exists (e.g., on read operations), verify via symlink resolution
	if realResult, err := filepath.EvalSymlinks(absResult); err == nil {
		// Path exists - verify the resolved path is still within base
		if !strings.HasPrefix(realResult, absBase+string(filepath.Separator)) {
			return "", fmt.Errorf("%w: path escapes base directory via symlink", ErrInvalidKey)
		}
	}

	// Ensure the result path is within the base path
	if !strings.HasPrefix(absResult, absBase+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: path escapes base directory", ErrInvalidKey)
	}

	return result, nil
}

// CreateBucket creates a new bucket
func (fs *FilesystemStorage) CreateBucket(name string) error {
	if err := ValidateBucketName(name); err != nil {
		return err
	}

	bucketPath := filepath.Join(fs.basePath, "buckets", name, "objects")

	// Check if bucket already exists
	if _, err := os.Stat(bucketPath); err == nil {
		return ErrBucketAlreadyExists
	}

	if err := os.MkdirAll(bucketPath, 0700); err != nil {
		return fmt.Errorf("creating bucket directory: %w", err)
	}

	return nil
}

// BucketExists checks if a bucket exists
func (fs *FilesystemStorage) BucketExists(name string) (bool, error) {
	if err := ValidateBucketName(name); err != nil {
		return false, err
	}

	bucketPath := filepath.Join(fs.basePath, "buckets", name, "objects")
	_, err := os.Stat(bucketPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking bucket existence: %w", err)
}

// DeleteBucket deletes a bucket (must be empty)
func (fs *FilesystemStorage) DeleteBucket(name string) error {
	if err := ValidateBucketName(name); err != nil {
		return err
	}

	bucketPath := filepath.Join(fs.basePath, "buckets", name)
	objectsPath := filepath.Join(bucketPath, "objects")

	// Check if bucket exists
	if _, err := os.Stat(objectsPath); os.IsNotExist(err) {
		return ErrBucketNotFound
	}

	// Check if bucket is empty by looking for any files in objects directory
	entries, err := os.ReadDir(objectsPath)
	if err != nil {
		return fmt.Errorf("reading bucket contents: %w", err)
	}
	if len(entries) > 0 {
		return ErrBucketNotEmpty
	}

	// Remove the bucket directory
	if err := os.RemoveAll(bucketPath); err != nil {
		return fmt.Errorf("removing bucket directory: %w", err)
	}

	return nil
}

// PutObject stores an object with the given key
func (fs *FilesystemStorage) PutObject(bucket, key string, contentType string, metadata map[string]string, body io.Reader) (*s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, err
	}
	dataPath := filepath.Join(objPath, "data")
	metaPath := filepath.Join(objPath, "meta.json")

	// Create object directory
	if err := os.MkdirAll(objPath, 0700); err != nil {
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
func (fs *FilesystemStorage) GetObject(bucket, key string) (io.ReadCloser, *s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	dataPath := filepath.Join(objPath, "data")

	// Get metadata first
	meta, err := fs.HeadObject(bucket, key)
	if err != nil {
		return nil, nil, err
	}

	// Open data file
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrObjectNotFound
		}
		return nil, nil, fmt.Errorf("opening object data: %w", err)
	}

	return file, meta, nil
}

// HeadObject retrieves object metadata without the body
func (fs *FilesystemStorage) HeadObject(bucket, key string) (*s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, err
	}
	metaPath := filepath.Join(objPath, "meta.json")

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrObjectNotFound
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
func (fs *FilesystemStorage) DeleteObject(bucket, key string) error {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return err
	}

	// Remove the entire object directory
	err = os.RemoveAll(objPath)
	if err != nil {
		return fmt.Errorf("removing object: %w", err)
	}

	// Try to remove empty parent directory (ignore errors)
	parentDir := filepath.Dir(objPath)
	_ = os.Remove(parentDir) // only succeeds if empty

	return nil
}

// ObjectExists checks if an object exists
func (fs *FilesystemStorage) ObjectExists(bucket, key string) (bool, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return false, err
	}
	metaPath := filepath.Join(objPath, "meta.json")

	_, err = os.Stat(metaPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("checking object existence: %w", err)
}

// GetObjectRange retrieves a range of bytes from an object
func (fs *FilesystemStorage) GetObjectRange(bucket, key string, start, end int64) (io.ReadCloser, *s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	dataPath := filepath.Join(objPath, "data")

	// Get metadata first
	meta, err := fs.HeadObject(bucket, key)
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
			return nil, nil, ErrObjectNotFound
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
func (fs *FilesystemStorage) ListObjects(bucket string, opts ListObjectsOptions) (*ListObjectsResult, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if opts.MaxKeys <= 0 {
		opts.MaxKeys = 1000
	}
	if opts.MaxKeys > 1000 {
		opts.MaxKeys = 1000
	}

	objectsPath := filepath.Join(fs.basePath, "buckets", bucket, "objects")
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
func (fs *FilesystemStorage) CopyObject(srcBucket, srcKey, dstBucket, dstKey string) (*s3.ObjectMetadata, error) {
	// Get source object
	srcReader, srcMeta, err := fs.GetObject(srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	defer srcReader.Close()

	// Copy to destination
	dstMeta, err := fs.PutObject(dstBucket, dstKey, srcMeta.ContentType, srcMeta.UserMetadata, srcReader)
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
