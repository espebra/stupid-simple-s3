package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// ErrInvalidUploadID is returned when an upload ID is not a valid UUID
var ErrInvalidUploadID = errors.New("invalid upload ID")

// uploadDir returns the directory of a multipart upload. Upload IDs from
// requests are validated as UUIDs before they get here, but the ID is also
// cleaned as a rooted path, so that the directory stays inside the multipart
// directory whatever the ID contains. Code scanning (CodeQL go/path-injection)
// recognizes filepath.Clean("/" + id) as sanitizing the ID, which it does not
// for the UUID check in validateUploadID.
func (fs *FilesystemStorage) uploadDir(uploadID string) string {
	return filepath.Join(fs.multipartPath, filepath.Clean("/"+uploadID))
}

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
	uploadPath := fs.uploadDir(uploadID)

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
	metaFile, err := create(metaPath)
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
	mu := fs.getUploadLock(uploadID)
	mu.RLock()
	defer mu.RUnlock()

	uploadPath := fs.uploadDir(uploadID)

	// Check upload exists
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return nil, ErrUploadNotFound
	}

	// Write part to file
	// The part number is an int, so the file name cannot contain a path
	// separator or "..".
	partFilename := fmt.Sprintf("part.%05d", partNumber)
	partPath := filepath.Join(uploadPath, partFilename)
	// A unique temp file per request keeps concurrent uploads of the same
	// part from writing into each other's data.
	tmpID := uuid.New().String()
	tmpPath := partPath + ".tmp." + tmpID

	tmpFile, err := create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating part file: %w", err)
	}

	// Calculate SHA-256 while writing
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	size, err := copyBuffered(writer, body)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing part data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("closing part file: %w", err)
	}

	// Save part metadata
	sum := hash.Sum(nil)
	partMeta := &s3.PartMetadata{
		PartNumber:     partNumber,
		ETag:           makeETag(sum, 0),
		Size:           size,
		ChecksumSHA256: EncodeChecksum(sum),
	}
	partMetaPath := filepath.Join(uploadPath, partFilename+".meta")
	partMetaTmpPath := partMetaPath + ".tmp." + tmpID
	if err := writeJSONFile(partMetaTmpPath, partMeta); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing part metadata: %w", err)
	}

	// Commit the data and metadata together, so that concurrent uploads of
	// the same part cannot leave the data of one and the metadata of another
	commit := fs.commitLock(uploadPath + "/" + partFilename)
	commit.Lock()
	defer commit.Unlock()

	if err := os.Rename(tmpPath, partPath); err != nil {
		_ = os.Remove(tmpPath)
		_ = os.Remove(partMetaTmpPath)
		return nil, fmt.Errorf("renaming part file: %w", err)
	}
	if err := os.Rename(partMetaTmpPath, partMetaPath); err != nil {
		_ = os.Remove(partMetaTmpPath)
		return nil, fmt.Errorf("renaming part metadata: %w", err)
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
	mu := fs.getUploadLock(uploadID)
	mu.Lock()
	defer mu.Unlock()

	uploadPath := fs.uploadDir(uploadID)

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

	// Verify all parts exist and ETags match, collecting the part digests for
	// the composite checksum
	var totalSize int64
	compositeHash := sha256.New()

	for i, part := range parts {
		// The part number is an int, so the file name cannot contain a path
		// separator or "..".
		partFilename := fmt.Sprintf("part.%05d", part.PartNumber)
		partPath := filepath.Join(uploadPath, partFilename)
		partMetaPath := filepath.Join(uploadPath, partFilename+".meta")

		var partMeta s3.PartMetadata
		if err := readJSONFile(partMetaPath, &partMeta); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("part %d: %w", part.PartNumber, ErrPartNotFound)
			}
			return nil, fmt.Errorf("reading part %d metadata: %w", part.PartNumber, err)
		}

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

		// Every part except the last must be at least the minimum part size
		if i < len(parts)-1 && partMeta.Size < fs.minPartSize {
			return nil, fmt.Errorf("part %d is %d bytes, minimum is %d: %w", part.PartNumber, partMeta.Size, fs.minPartSize, ErrEntityTooSmall)
		}

		partSum, err := partDigest(&partMeta, partPath)
		if err != nil {
			return nil, fmt.Errorf("part %d checksum: %w", part.PartNumber, err)
		}
		compositeHash.Write(partSum)
		totalSize += partMeta.Size
	}

	// The object directory is created only after the object has been
	// assembled in the temp directory (prepareObjectDir)
	objPath, keyErr := fs.keyToPath(uploadMeta.Bucket, uploadMeta.Key)
	if keyErr != nil {
		return nil, keyErr
	}

	// Keep the bucket from being deleted until the object is committed
	bucketLock := fs.bucketLock(uploadMeta.Bucket)
	bucketLock.RLock()
	defer bucketLock.RUnlock()

	// Concatenate all parts. Copying file to file lets the kernel use
	// copy_file_range, which avoids user-space copies and can share extents
	// on filesystems that support reflinks.
	outFile, tmpPath, metaTmpPath, err := fs.createTempFiles(uploadMeta.Bucket)
	if err != nil {
		return nil, err
	}

	for _, part := range parts {
		// The part number is an int, so the file name cannot contain a path
		// separator or "..".
		partFilename := fmt.Sprintf("part.%05d", part.PartNumber)
		partPath := filepath.Join(uploadPath, partFilename)

		partFile, err := open(partPath)
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

	// Create object metadata
	now := time.Now().UTC()
	compositeSum := compositeHash.Sum(nil)
	objMeta := &s3.ObjectMetadata{
		Key:            uploadMeta.Key,
		Size:           totalSize,
		ContentType:    uploadMeta.ContentType,
		ETag:           makeETag(compositeSum, len(parts)),
		LastModified:   now,
		UserMetadata:   uploadMeta.UserMetadata,
		ChecksumSHA256: fmt.Sprintf("%s-%d", EncodeChecksum(compositeSum), len(parts)),
	}

	if err := writeJSONFile(metaTmpPath, objMeta); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	if err := fs.prepareObjectDir(objPath); err != nil {
		_ = os.Remove(tmpPath)
		_ = os.Remove(metaTmpPath)
		return nil, err
	}
	if err := fs.commitObject(uploadMeta.Bucket, objPath, tmpPath, metaTmpPath, objMeta); err != nil {
		return nil, err
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
	mu := fs.getUploadLock(uploadID)
	mu.Lock()
	defer mu.Unlock()

	uploadPath := fs.uploadDir(uploadID)

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
	mu := fs.getUploadLock(uploadID)
	mu.RLock()
	defer mu.RUnlock()
	return fs.getMultipartUploadInternal(uploadID)
}

// getMultipartUploadInternal retrieves metadata without acquiring lock (caller must hold lock)
func (fs *FilesystemStorage) getMultipartUploadInternal(uploadID string) (*s3.MultipartUploadMetadata, error) {
	// Guard against unvalidated IDs reaching a path expression, in case a future
	// caller forgets the validateUploadID check its current callers perform.
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}

	uploadPath := fs.uploadDir(uploadID)
	metaPath := filepath.Join(uploadPath, "meta.json")

	var meta s3.MultipartUploadMetadata
	if err := readJSONFile(metaPath, &meta); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrUploadNotFound
		}
		return nil, fmt.Errorf("reading upload metadata: %w", err)
	}

	return &meta, nil
}

// ListParts returns the parts uploaded for a multipart upload
func (fs *FilesystemStorage) ListParts(uploadID string) ([]s3.PartMetadata, error) {
	if err := validateUploadID(uploadID); err != nil {
		return nil, ErrUploadNotFound
	}
	mu := fs.getUploadLock(uploadID)
	mu.RLock()
	defer mu.RUnlock()

	uploadPath := fs.uploadDir(uploadID)

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

		var partMeta s3.PartMetadata
		if err := readJSONFile(filepath.Join(uploadPath, name), &partMeta); err != nil {
			continue
		}

		partMeta.PartNumber = partNum
		if info, err := os.Stat(filepath.Join(uploadPath, strings.TrimSuffix(name, ".meta"))); err == nil {
			partMeta.LastModified = info.ModTime().UTC()
		}
		parts = append(parts, partMeta)
	}

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// ListMultipartUploads returns the in-progress multipart uploads in a bucket
// whose keys start with prefix, sorted by key and then upload ID. Sorting by
// upload ID rather than initiation time within a key matches how S3 applies
// upload-id-marker, so pagination never skips or repeats an upload.
func (fs *FilesystemStorage) ListMultipartUploads(bucket, prefix string) ([]s3.MultipartUploadMetadata, error) {
	entries, err := os.ReadDir(fs.multipartPath)
	if err != nil {
		return nil, fmt.Errorf("reading multipart directory: %w", err)
	}

	var uploads []s3.MultipartUploadMetadata
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// GetMultipartUpload validates the name as an upload ID. Uploads that
		// are completed, aborted or still being created while listing are
		// skipped.
		meta, err := fs.GetMultipartUpload(entry.Name())
		if err != nil {
			continue
		}
		if meta.Bucket != bucket || !strings.HasPrefix(meta.Key, prefix) {
			continue
		}
		uploads = append(uploads, *meta)
	}

	sort.Slice(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].UploadID < uploads[j].UploadID
	})
	return uploads, nil
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

	// Evict lock map entries for uploads that no longer exist on disk
	// (completed, aborted, or just cleaned up above).
	fs.uploadLocks.Range(func(key, value any) bool {
		uploadID := key.(string)
		dir := fs.uploadDir(uploadID)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			fs.uploadLocks.Delete(key)
		}
		return true
	})

	return cleaned, nil
}

// cleanupUploadIfStale checks if an upload is stale and removes it atomically
func (fs *FilesystemStorage) cleanupUploadIfStale(uploadID string, cutoff time.Time, entry os.DirEntry) bool {
	mu := fs.getUploadLock(uploadID)
	mu.Lock()
	defer mu.Unlock()

	uploadPath := fs.uploadDir(uploadID)

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

// CleanupStaleTempFiles removes orphaned temp files from the buckets' temp
// directories. These can be left behind by PutObject or
// CompleteMultipartUpload when the process stops after creating the temp
// file but before renaming it. Only the temp directories are read, not the
// object directories, so the cost does not grow with the number of objects.
func (fs *FilesystemStorage) CleanupStaleTempFiles(maxAge time.Duration) (int, error) {
	bucketsPath := filepath.Join(fs.basePath, "buckets")
	cutoff := time.Now().Add(-maxAge)
	cleaned := 0

	buckets, err := os.ReadDir(bucketsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading buckets directory: %w", err)
	}
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		tmpDir := filepath.Join(bucketsPath, bucket.Name(), "tmp")
		entries, err := os.ReadDir(tmpDir)
		if err != nil {
			continue // no temp directory yet, or the bucket was just deleted
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil || entry.IsDir() {
				continue
			}
			if info.ModTime().Before(cutoff) {
				if os.Remove(filepath.Join(tmpDir, entry.Name())) == nil {
					cleaned++
				}
			}
		}
	}
	return cleaned, nil
}

// partDigest returns the raw SHA-256 digest of a part. Parts uploaded before
// SHA-256 checksums were introduced have none stored, so their data is hashed.
func partDigest(partMeta *s3.PartMetadata, partPath string) ([]byte, error) {
	if partMeta.ChecksumSHA256 != "" {
		return base64.StdEncoding.DecodeString(partMeta.ChecksumSHA256)
	}
	// partPath is safe to open even though it is built from request data:
	// CompleteMultipartUpload rejects any upload ID that is not a UUID
	// (validateUploadID) and builds the directory with uploadDir, and the
	// part number is an int formatted as "part.%05d", so neither can contain
	// a path separator or "..".
	f, err := open(partPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	hash := sha256.New()
	if _, err := io.Copy(hash, f); err != nil {
		return nil, err
	}
	return hash.Sum(nil), nil
}

// metaBufferSize is the size of the buffers in metaBufferPool. Metadata
// files are a few hundred bytes, more with user metadata.
const metaBufferSize = 4096

// metaBufferPool holds the buffers that readJSONFile reads files into
var metaBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, metaBufferSize)
		return &buf
	},
}

// readJSONFile decodes the JSON file at path into v. Metadata files are
// small, so it reads the file with one read into a pooled buffer, where
// json.NewDecoder allocates a decoder and a read buffer that it grows as it
// goes. A file that does not fit in the buffer is read to the end. v must
// not keep references to the data it is decoded from; json.Unmarshal copies
// strings, so structs of strings, numbers, times and maps are fine. A
// missing file gives an error that matches os.ErrNotExist.
func readJSONFile(path string, v any) error {
	f, err := open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	bufp := metaBufferPool.Get().(*[]byte)
	defer metaBufferPool.Put(bufp)
	buf := *bufp

	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return err
	}
	// A short read of a regular file normally ends at the end of the file.
	// A JSON object is only valid up to its closing brace, so if it parses,
	// nothing but trailing whitespace can be missing.
	if n < len(buf) && json.Unmarshal(buf[:n], v) == nil {
		return nil
	}
	rest, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	return json.Unmarshal(append(buf[:n:n], rest...), v)
}

// writeJSONFile writes v as JSON to a new file at path
func writeJSONFile(path string, v any) error {
	f, err := create(path)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(f).Encode(v); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
