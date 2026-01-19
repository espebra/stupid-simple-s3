package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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

// CreateMultipartUpload initializes a new multipart upload
func (fs *FilesystemStorage) CreateMultipartUpload(key string, contentType string, metadata map[string]string) (string, error) {
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
		Key:          key,
		Created:      time.Now().UTC(),
		ContentType:  contentType,
		UserMetadata: metadata,
	}

	metaPath := filepath.Join(uploadPath, "meta.json")
	metaFile, err := os.Create(metaPath)
	if err != nil {
		os.RemoveAll(uploadPath)
		return "", fmt.Errorf("creating upload metadata: %w", err)
	}
	defer metaFile.Close()

	if err := json.NewEncoder(metaFile).Encode(uploadMeta); err != nil {
		os.RemoveAll(uploadPath)
		return "", fmt.Errorf("writing upload metadata: %w", err)
	}

	return uploadID, nil
}

// UploadPart stores a part of a multipart upload
func (fs *FilesystemStorage) UploadPart(uploadID string, partNumber int, body io.Reader) (*s3.PartMetadata, error) {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	// Check upload exists
	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("upload not found: %s", uploadID)
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
		tmpFile.Close()
		os.Remove(tmpPath)
		return nil, fmt.Errorf("writing part data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("closing part file: %w", err)
	}

	if err := os.Rename(tmpPath, partPath); err != nil {
		os.Remove(tmpPath)
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
	defer partMetaFile.Close()

	if err := json.NewEncoder(partMetaFile).Encode(partMeta); err != nil {
		return nil, fmt.Errorf("writing part metadata: %w", err)
	}

	return partMeta, nil
}

// CompleteMultipartUpload assembles all parts into the final object
func (fs *FilesystemStorage) CompleteMultipartUpload(uploadID string, parts []s3.CompletedPartInput) (*s3.ObjectMetadata, error) {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	// Get upload metadata
	uploadMeta, err := fs.GetMultipartUpload(uploadID)
	if err != nil {
		return nil, err
	}

	// Validate parts are in order
	for i := 1; i < len(parts); i++ {
		if parts[i].PartNumber <= parts[i-1].PartNumber {
			return nil, fmt.Errorf("parts not in ascending order")
		}
	}

	// Verify all parts exist and ETags match
	var partHashes [][]byte
	var totalSize int64

	for _, part := range parts {
		partFilename := fmt.Sprintf("part.%05d", part.PartNumber)
		partMetaPath := filepath.Join(uploadPath, partFilename+".meta")

		partMetaFile, err := os.Open(partMetaPath)
		if err != nil {
			return nil, fmt.Errorf("part %d not found", part.PartNumber)
		}

		var partMeta s3.PartMetadata
		if err := json.NewDecoder(partMetaFile).Decode(&partMeta); err != nil {
			partMetaFile.Close()
			return nil, fmt.Errorf("reading part %d metadata: %w", part.PartNumber, err)
		}
		partMetaFile.Close()

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
	objPath, keyErr := fs.keyToPath(uploadMeta.Key)
	if keyErr != nil {
		return nil, keyErr
	}
	if err := os.MkdirAll(objPath, 0700); err != nil {
		return nil, fmt.Errorf("creating object directory: %w", err)
	}

	dataPath := filepath.Join(objPath, "data")
	tmpPath := dataPath + ".tmp"

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
			outFile.Close()
			os.Remove(tmpPath)
			return nil, fmt.Errorf("opening part %d: %w", part.PartNumber, err)
		}

		_, err = io.Copy(outFile, partFile)
		partFile.Close()
		if err != nil {
			outFile.Close()
			os.Remove(tmpPath)
			return nil, fmt.Errorf("copying part %d: %w", part.PartNumber, err)
		}
	}

	if err := outFile.Close(); err != nil {
		os.Remove(tmpPath)
		return nil, fmt.Errorf("closing output file: %w", err)
	}

	if err := os.Rename(tmpPath, dataPath); err != nil {
		os.Remove(tmpPath)
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

	// Write metadata
	metaPath := filepath.Join(objPath, "meta.json")
	metaFile, err := os.Create(metaPath)
	if err != nil {
		return nil, fmt.Errorf("creating metadata file: %w", err)
	}
	defer metaFile.Close()

	if err := json.NewEncoder(metaFile).Encode(objMeta); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	// Clean up multipart upload directory
	os.RemoveAll(uploadPath)

	return objMeta, nil
}

// AbortMultipartUpload cancels a multipart upload and cleans up parts
func (fs *FilesystemStorage) AbortMultipartUpload(uploadID string) error {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	if _, err := os.Stat(uploadPath); os.IsNotExist(err) {
		return fmt.Errorf("upload not found: %s", uploadID)
	}

	if err := os.RemoveAll(uploadPath); err != nil {
		return fmt.Errorf("removing upload: %w", err)
	}

	return nil
}

// GetMultipartUpload retrieves metadata about a multipart upload
func (fs *FilesystemStorage) GetMultipartUpload(uploadID string) (*s3.MultipartUploadMetadata, error) {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)
	metaPath := filepath.Join(uploadPath, "meta.json")

	metaFile, err := os.Open(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("upload not found: %s", uploadID)
		}
		return nil, fmt.Errorf("opening upload metadata: %w", err)
	}
	defer metaFile.Close()

	var meta s3.MultipartUploadMetadata
	if err := json.NewDecoder(metaFile).Decode(&meta); err != nil {
		return nil, fmt.Errorf("parsing upload metadata: %w", err)
	}

	return &meta, nil
}

// ListParts returns the parts uploaded for a multipart upload
func (fs *FilesystemStorage) ListParts(uploadID string) ([]s3.PartMetadata, error) {
	uploadPath := filepath.Join(fs.multipartPath, uploadID)

	entries, err := os.ReadDir(uploadPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("upload not found: %s", uploadID)
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
			metaFile.Close()
			continue
		}
		metaFile.Close()

		partMeta.PartNumber = partNum
		parts = append(parts, partMeta)
	}

	// Sort by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

// CleanupStaleUploads removes multipart uploads older than maxAge
// Returns the number of uploads cleaned up
func (fs *FilesystemStorage) CleanupStaleUploads(maxAge time.Duration) (int, error) {
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
		uploadMeta, err := fs.GetMultipartUpload(uploadID)
		if err != nil {
			// If we can't read metadata, check directory modification time
			info, statErr := entry.Info()
			if statErr != nil {
				continue
			}
			if info.ModTime().Before(cutoff) {
				uploadPath := filepath.Join(fs.multipartPath, uploadID)
				if removeErr := os.RemoveAll(uploadPath); removeErr == nil {
					cleaned++
				}
			}
			continue
		}

		if uploadMeta.Created.Before(cutoff) {
			if abortErr := fs.AbortMultipartUpload(uploadID); abortErr == nil {
				cleaned++
			}
		}
	}

	return cleaned, nil
}
