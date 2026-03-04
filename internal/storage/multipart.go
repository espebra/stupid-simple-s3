package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// ErrInvalidUploadID is returned when an upload ID is not a valid UUID
var ErrInvalidUploadID = errors.New("invalid upload ID")

// validateUploadID checks that the upload ID is a valid UUID.
// This prevents path traversal attacks since upload IDs are used in filesystem paths.
func validateUploadID(uploadID string) error {
	if err := uuid.Validate(uploadID); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidUploadID, uploadID)
	}
	return nil
}

// CreateMultipartUpload initializes a new multipart upload
func (fs *FilesystemStorage) CreateMultipartUpload(bucket, key string, contentType string, metadata map[string]string) (string, error) {
	// Validate the bucket name upfront
	if err := ValidateBucketName(bucket); err != nil {
		return "", err
	}

	// Validate the key upfront to fail early
	if err := ValidateKey(key); err != nil {
		return "", err
	}

	uploadID := uuid.New().String()
	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	if err := os.MkdirAll(uploadPath, 0700); err != nil {
		return "", fmt.Errorf("creating upload directory: %w", err)
	}

	uploadMeta := &s3.MultipartUploadMetadata{
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		Created:      time.Now().UTC(),
		ContentType:  contentType,
		UserMetadata: metadata,
	}

	metaPath := filepath.Join(uploadPath, "meta.json")
	metaFile, err := os.Create(metaPath)
	if err != nil {
		_ = os.RemoveAll(uploadPath)
		return "", fmt.Errorf("creating upload metadata: %w", err)
	}
	defer func() { _ = metaFile.Close() }()

	if err := json.NewEncoder(metaFile).Encode(uploadMeta); err != nil {
		_ = os.RemoveAll(uploadPath)
		return "", fmt.Errorf("writing upload metadata: %w", err)
	}

	return uploadID, nil
}

// UploadPart stores a part of a multipart upload
func (fs *FilesystemStorage) UploadPart(uploadID string, partNumber int, body io.Reader) (*s3.PartMetadata, error) {
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}
	// Per-upload read lock: allows concurrent part uploads for the same upload
	// while preventing deletion/completion of this specific upload.
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.RLock()
	defer func() {
		ul.mu.RUnlock()
		fs.releaseUploadLock(uploadID)
	}()

	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	// Check upload exists
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return nil, ErrUploadNotFound
	}

	// Write part to file
	partFilename := fmt.Sprintf("part.%05d", partNumber)
	partPath := filepath.Join(uploadPath, partFilename)
	tmpPath := partPath + ".tmp"

	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating part file: %w", err)
	}

	// Calculate MD5 while writing
	hash := md5.New()
	writer := io.MultiWriter(tmpFile, hash)

	size, err := io.Copy(writer, body)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing part data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("closing part file: %w", err)
	}

	if err := os.Rename(tmpPath, partPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("renaming part file: %w", err)
	}

	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(hash.Sum(nil)))

	// Save part metadata
	partMeta := &s3.PartMetadata{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       size,
	}

	partMetaPath := filepath.Join(uploadPath, partFilename+".meta")
	partMetaFile, err := os.Create(partMetaPath)
	if err != nil {
		return nil, fmt.Errorf("creating part metadata: %w", err)
	}
	defer func() { _ = partMetaFile.Close() }()

	if err := json.NewEncoder(partMetaFile).Encode(partMeta); err != nil {
		return nil, fmt.Errorf("writing part metadata: %w", err)
	}

	return partMeta, nil
}

// CompleteMultipartUpload assembles all parts into the final object
func (fs *FilesystemStorage) CompleteMultipartUpload(uploadID string, parts []s3.CompletedPartInput) (*s3.ObjectMetadata, error) {
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}
	// Per-upload exclusive lock: waits for in-flight part uploads to finish,
	// then prevents new ones while we assemble the final object.
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.Lock()
	defer func() {
		ul.mu.Unlock()
		fs.releaseUploadLock(uploadID)
	}()

	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	// Get upload metadata (internal call, lock already held)
	uploadMeta, err := fs.getMultipartUploadInternal(uploadID)
	if err != nil {
		return nil, err
	}

	// Validate parts are in order
	for i := 1; i < len(parts); i++ {
		if parts[i].PartNumber <= parts[i-1].PartNumber {
			return nil, ErrInvalidPartOrder
		}
	}

	// Verify all parts exist and ETags match
	var partHashes [][]byte
	var totalSize int64

	for _, part := range parts {
		partFilename := fmt.Sprintf("part.%05d", part.PartNumber)
		partPath := filepath.Join(uploadPath, partFilename)
		partMetaPath := filepath.Join(uploadPath, partFilename+".meta")

		partMetaFile, err := os.Open(partMetaPath)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", part.PartNumber, ErrPartNotFound)
		}

		var partMeta s3.PartMetadata
		if err := json.NewDecoder(partMetaFile).Decode(&partMeta); err != nil {
			_ = partMetaFile.Close()
			return nil, fmt.Errorf("reading part %d metadata: %w", part.PartNumber, err)
		}
		_ = partMetaFile.Close()

		// Verify actual part file size matches metadata
		partInfo, err := os.Stat(partPath)
		if err != nil {
			return nil, fmt.Errorf("part %d: %w", part.PartNumber, ErrPartNotFound)
		}
		if partInfo.Size() != partMeta.Size {
			return nil, fmt.Errorf("part %d size mismatch: metadata claims %d bytes, file has %d bytes", part.PartNumber, partMeta.Size, partInfo.Size())
		}

		// Normalize ETags for comparison (remove quotes if present)
		expectedETag := strings.Trim(part.ETag, "\"")
		actualETag := strings.Trim(partMeta.ETag, "\"")

		if expectedETag != actualETag {
			return nil, fmt.Errorf("part %d ETag mismatch: expected %s, got %s", part.PartNumber, expectedETag, actualETag)
		}

		// Decode the hex MD5 for multipart ETag calculation
		hashBytes, err := hex.DecodeString(actualETag)
		if err != nil {
			return nil, fmt.Errorf("invalid part %d ETag format: %w", part.PartNumber, err)
		}
		partHashes = append(partHashes, hashBytes)
		totalSize += partMeta.Size
	}

	// Create object directory
	objPath, keyErr := fs.keyToPath(uploadMeta.Bucket, uploadMeta.Key)
	if keyErr != nil {
		return nil, keyErr
	}
	if err := os.MkdirAll(objPath, 0700); err != nil {
		return nil, fmt.Errorf("creating object directory: %w", err)
	}

	dataPath := filepath.Join(objPath, "data")
	tmpPath := dataPath + ".tmp." + uploadID

	// Concatenate all parts
	outFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating output file: %w", err)
	}

	for _, part := range parts {
		partFilename := fmt.Sprintf("part.%05d", part.PartNumber)
		partPath := filepath.Join(uploadPath, partFilename)

		partFile, err := os.Open(partPath)
		if err != nil {
			_ = outFile.Close()
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("opening part %d: %w", part.PartNumber, err)
		}

		_, err = io.Copy(outFile, partFile)
		_ = partFile.Close()
		if err != nil {
			_ = outFile.Close()
			_ = os.Remove(tmpPath)
			return nil, fmt.Errorf("copying part %d: %w", part.PartNumber, err)
		}
	}

	if err := outFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("closing output file: %w", err)
	}

	if err := os.Rename(tmpPath, dataPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("renaming output file: %w", err)
	}

	// Calculate multipart ETag: MD5 of concatenated part MD5s, with -N suffix
	combinedHash := md5.New()
	for _, h := range partHashes {
		combinedHash.Write(h)
	}
	etag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(combinedHash.Sum(nil)), len(parts))

	// Create object metadata
	now := time.Now().UTC()
	objMeta := &s3.ObjectMetadata{
		Key:          uploadMeta.Key,
		Size:         totalSize,
		ContentType:  uploadMeta.ContentType,
		ETag:         etag,
		LastModified: now,
		UserMetadata: uploadMeta.UserMetadata,
	}

	// Write metadata atomically using temp file and rename
	metaPath := filepath.Join(objPath, "meta.json")
	metaTmpPath := metaPath + ".tmp." + uploadID
	metaFile, err := os.Create(metaTmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating metadata file: %w", err)
	}

	if err := json.NewEncoder(metaFile).Encode(objMeta); err != nil {
		_ = metaFile.Close()
		_ = os.Remove(metaTmpPath)
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	if err := metaFile.Close(); err != nil {
		_ = os.Remove(metaTmpPath)
		return nil, fmt.Errorf("closing metadata file: %w", err)
	}

	if err := os.Rename(metaTmpPath, metaPath); err != nil {
		_ = os.Remove(metaTmpPath)
		return nil, fmt.Errorf("renaming metadata file: %w", err)
	}

	// Clean up multipart upload directory
	_ = os.RemoveAll(uploadPath)

	return objMeta, nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up parts
func (fs *FilesystemStorage) AbortMultipartUpload(uploadID string) error {
	if err := validateUploadID(uploadID); err != nil {
		return ErrUploadNotFound
	}
	// Per-upload exclusive lock: waits for in-flight part uploads to finish.
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.Lock()
	defer func() {
		ul.mu.Unlock()
		fs.releaseUploadLock(uploadID)
	}()

	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return ErrUploadNotFound
	}

	if err := os.RemoveAll(uploadPath); err != nil {
		return fmt.Errorf("removing upload: %w", err)
	}

	return nil
}

// GetMultipartUpload retrieves metadata about a multipart upload
func (fs *FilesystemStorage) GetMultipartUpload(uploadID string) (*s3.MultipartUploadMetadata, error) {
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.RLock()
	defer func() {
		ul.mu.RUnlock()
		fs.releaseUploadLock(uploadID)
	}()
	return fs.getMultipartUploadInternal(uploadID)
}

// getMultipartUploadInternal retrieves metadata without acquiring lock (caller must hold lock)
func (fs *FilesystemStorage) getMultipartUploadInternal(uploadID string) (*s3.MultipartUploadMetadata, error) {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)
	metaPath := filepath.Join(uploadPath, "meta.json")

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrUploadNotFound
		}
		return nil, fmt.Errorf("opening upload metadata: %w", err)
	}
	defer func() { _ = metaFile.Close() }()

	var meta s3.MultipartUploadMetadata
	if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return nil, fmt.Errorf("parsing upload metadata: %w", err)
	}

	return &meta, nil
}

// ListParts returns the parts uploaded for a multipart upload
func (fs *FilesystemStorage) ListParts(uploadID string) ([]s3.PartMetadata, error) {
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.RLock()
	defer func() {
		ul.mu.RUnlock()
		fs.releaseUploadLock(uploadID)
	}()

	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	entries, err := os.ReadDir(uploadPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrUploadNotFound
		}
		return nil, fmt.Errorf("reading upload directory: %w", err)
	}

	var parts []s3.PartMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "part.") || !strings.HasSuffix(name, ".meta") {
			continue
		}

		// Extract part number from filename
		numStr := strings.TrimPrefix(name, "part.")
		numStr = strings.TrimSuffix(numStr, ".meta")
		partNum, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}

		metaPath := filepath.Join(uploadPath, name)
		metaFile, err := os.Open(metaPath)
		if err != nil {
			continue
		}

		var partMeta s3.PartMetadata
		if err := json.NewDecoder(metaFile).Decode(&partMeta); err != nil {
			_ = metaFile.Close()
			continue
		}
		_ = metaFile.Close()

		partMeta.PartNumber = partNum
		parts = append(parts, partMeta)
	}

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// CountActiveUploads returns the number of active multipart uploads
// by counting directories in the multipart upload path.
func (fs *FilesystemStorage) CountActiveUploads() int {
	entries, err := os.ReadDir(fs.multipartPath)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			count++
		}
	}
	return count
}

// CleanupStaleUploads removes multipart uploads older than maxAge
// Returns the number of uploads cleaned up
func (fs *FilesystemStorage) CleanupStaleUploads(maxAge time.Duration) (int, error) {
	// Read directory listing without lock (just reading names)
	entries, err := os.ReadDir(fs.multipartPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading multipart directory: %w", err)
	}

	cutoff := time.Now().UTC().Add(-maxAge)
	cleaned := 0

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		uploadID := entry.Name()

		// Check and remove with lock held to prevent races
		if fs.cleanupUploadIfStale(uploadID, cutoff, entry) {
			cleaned++
		}
	}

	return cleaned, nil
}

// cleanupUploadIfStale checks if an upload is stale and removes it atomically
func (fs *FilesystemStorage) cleanupUploadIfStale(uploadID string, cutoff time.Time, entry os.DirEntry) bool {
	ul := fs.acquireUploadLock(uploadID)
	ul.mu.Lock()
	defer func() {
		ul.mu.Unlock()
		fs.releaseUploadLock(uploadID)
	}()

	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	// Check if still exists (might have been completed/aborted)
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return false
	}

	uploadMeta, err := fs.getMultipartUploadInternal(uploadID)
	if err != nil {
		// If we can't read metadata, check directory modification time
		info, statErr := entry.Info()
		if statErr != nil {
			return false
		}
		if info.ModTime().Before(cutoff) {
			if removeErr := os.RemoveAll(uploadPath); removeErr == nil {
				return true
			}
		}
		return false
	}

	if uploadMeta.Created.Before(cutoff) {
		if removeErr := os.RemoveAll(uploadPath); removeErr == nil {
			return true
		}
	}

	return false
}

// CleanupStaleTempFiles removes orphaned .tmp.* files from object directories.
// These can be left behind by PutObject or CompleteMultipartUpload that failed
// or was interrupted after creating the temp file but before renaming it.
func (fs *FilesystemStorage) CleanupStaleTempFiles(maxAge time.Duration) (int, error) {
	bucketsPath := filepath.Join(fs.basePath, "buckets")
	cutoff := time.Now().Add(-maxAge)
	cleaned := 0

	err := filepath.WalkDir(bucketsPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip directories we can't read
		}
		if d.IsDir() {
			return nil
		}

		name := d.Name()
		if !strings.Contains(name, ".tmp.") {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.ModTime().Before(cutoff) {
			if os.Remove(path) == nil {
				cleaned++
			}
		}
		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return cleaned, fmt.Errorf("walking buckets directory: %w", err)
	}
	return cleaned, nil
}
