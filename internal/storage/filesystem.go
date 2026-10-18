package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

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

// ErrEntityTooSmall is returned when a part other than the last is smaller than the minimum part size
var ErrEntityTooSmall = errors.New("entity too small")

// MinPartSize is the minimum size of every part of a multipart upload except the last, as in S3
const MinPartSize = 5 * 1024 * 1024

// ErrInvalidPartOrder is returned when parts are not in ascending order
var ErrInvalidPartOrder = errors.New("parts must be in ascending order")

// ErrBadDigest is returned when uploaded data does not match the checksum sent by the client
var ErrBadDigest = errors.New("checksum mismatch")

// EncodeChecksum returns a digest in the base64 form used by the
// x-amz-checksum-sha256 header.
func EncodeChecksum(sum []byte) string {
	return base64.StdEncoding.EncodeToString(sum)
}

// makeETag builds a quoted ETag from the SHA-256 of the data. SHA-256 is
// collision resistant, so the ETag only changes when the content does.
// The "sha256-" prefix keeps a "-" in every ETag: clients such as AWS SDK for
// Java v1 treat an ETag without one as an MD5 digest and fail to verify it.
// A non-zero parts count adds the S3 multipart "-N" suffix.
func makeETag(sum []byte, parts int) string {
	if parts > 0 {
		return fmt.Sprintf("\"sha256-%s-%d\"", hex.EncodeToString(sum), parts)
	}
	return fmt.Sprintf("\"sha256-%s\"", hex.EncodeToString(sum))
}

// ValidateKey checks that an object key is safe and doesn't contain path traversal sequences.
// Returns an error if the key is invalid.
func ValidateKey(key string) error {
	// Reject empty keys
	if key == "" {
		return fmt.Errorf("%w: key cannot be empty", ErrInvalidKey)
	}

	// Reject keys longer than 1024 bytes (S3 limit)
	if len(key) > 1024 {
		return fmt.Errorf("%w: key cannot exceed 1024 bytes", ErrInvalidKey)
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
	basePath string
	// cleanBase is basePath cleaned, as it appears at the start of the
	// paths built from it with filepath.Join
	cleanBase string
	// realBase is basePath made absolute with symlinks resolved, at startup
	realBase      string
	multipartPath string
	// minPartSize is the minimum size of all parts but the last when completing
	// a multipart upload. It is MinPartSize except in tests.
	minPartSize int64
	// uploadLocks holds a per-upload RWMutex so that operations on different
	// uploads never block each other.
	uploadLocks sync.Map // map[string]*sync.RWMutex
	// commitLocks serialize the short commit step (renames and metadata
	// writes) of writes to the same object or part, which hold the write
	// lock. Readers hold the read lock while opening an object's data and
	// metadata, so they always get both from the same write. Names are hashed
	// to a fixed set of locks, so no per-name state accumulates. Only one is
	// ever held at a time, so they cannot deadlock.
	commitLocks [256]sync.RWMutex

	// bucketLocks holds a per-bucket RWMutex. Writes hold the read lock from
	// creating their temp files until they commit; DeleteBucket takes the
	// write lock, so it never removes the temp files of a write in progress.
	bucketLocks sync.Map // map[string]*sync.RWMutex

	// indexes holds the listing index of each bucket listed since startup
	indexMu sync.Mutex
	indexes map[string]*bucketIndex
}

// bucketLock returns the lock that keeps a bucket from being deleted while
// writes to it are in progress
func (fs *FilesystemStorage) bucketLock(bucket string) *sync.RWMutex {
	lock, _ := fs.bucketLocks.LoadOrStore(bucket, &sync.RWMutex{})
	return lock.(*sync.RWMutex)
}

// commitLock returns the lock that serializes commits for the given name
func (fs *FilesystemStorage) commitLock(name string) *sync.RWMutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return &fs.commitLocks[h.Sum32()%uint32(len(fs.commitLocks))]
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

	// Resolve the base path once. Object paths are checked against it on
	// every request, so resolving it there would cost a stat per path
	// component on every request.
	realBase, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("resolving base path: %w", err)
	}
	// Resolve symlinks, for cases like /tmp -> /private/tmp on macOS
	if resolved, err := filepath.EvalSymlinks(realBase); err == nil {
		realBase = resolved
	}

	return &FilesystemStorage{
		basePath:      basePath,
		cleanBase:     filepath.Clean(basePath),
		realBase:      realBase,
		multipartPath: multipartPath,
		minPartSize:   MinPartSize,
	}, nil
}

// keyToPath converts an object key to a filesystem path within a bucket
// Uses a 4-character hash prefix for directory distribution (65,536 directories) and SHA-256 hex directory name
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

	// SHA-256 hash of the key: first 2 bytes for shard prefix, full hash for directory name
	keyHash := sha256.Sum256([]byte(key))
	prefix := hex.EncodeToString(keyHash[:2])
	encodedKey := hex.EncodeToString(keyHash[:])

	// Clean the path below the buckets directory as a rooted path, so that it
	// stays inside the buckets directory whatever the bucket name contains.
	// For valid names this changes nothing. Code scanning (CodeQL
	// go/path-injection) recognizes filepath.Clean("/" + p) as sanitizing p,
	// which it does not for ValidateBucketName or the containment check below.
	rel := filepath.Join(bucket, "objects", prefix, encodedKey)
	result := filepath.Join(fs.basePath, "buckets", filepath.Clean("/"+rel))

	// Defense in depth: verify the resulting path is within the base path.
	// This is plain path arithmetic against the base path resolved at
	// startup, with no system calls. Symlinks inside the storage tree are
	// checked when a write creates its object directory (prepareObjectDir).
	absResult := filepath.Join(fs.realBase, "buckets", filepath.Clean("/"+rel))
	if !strings.HasPrefix(absResult, fs.realBase+string(filepath.Separator)) {
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
	if err := os.Mkdir(fs.bucketTmpDir(name), 0700); err != nil && !os.IsExist(err) {
		return fmt.Errorf("creating bucket temp directory: %w", err)
	}

	return nil
}

// bucketTmpDir returns the directory where writes to a bucket keep their
// temp files until they are renamed into the object directory. Keeping them
// in one directory per bucket, on the same filesystem as the objects, lets
// CleanupStaleTempFiles find leftovers without walking the whole bucket.
// The bucket name is cleaned as a rooted path, as in keyToPath.
func (fs *FilesystemStorage) bucketTmpDir(bucket string) string {
	return filepath.Join(fs.basePath, "buckets", filepath.Clean("/"+bucket), "tmp")
}

// newTempFilePaths returns paths for the data and metadata temp files of a
// write to a bucket, creating the bucket's temp directory if needed (buckets
// created before it existed get it on their first write)
func (fs *FilesystemStorage) newTempFilePaths(bucket string) (dataTmpPath, metaTmpPath string, err error) {
	dir := fs.bucketTmpDir(bucket)
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		if isMissingDirError(err) {
			return "", "", ErrBucketNotFound
		}
		return "", "", fmt.Errorf("creating bucket temp directory: %w", err)
	}
	id := uuid.New().String()
	return filepath.Join(dir, id+".data"), filepath.Join(dir, id+".meta"), nil
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

// prepareObjectDir creates the directory of an object that is about to be
// committed and checks that it is inside the base path. Writes call it only
// after their temp files are complete, so that a failed write does not
// leave an empty object directory, which would keep the bucket from being
// deleted.
func (fs *FilesystemStorage) prepareObjectDir(objPath string) error {
	if err := createObjectDir(objPath); err != nil {
		return err
	}

	// Defense in depth: verify the created directory is within the base
	// path after resolving symlinks, so that a symlink planted in the
	// storage tree cannot make a write land outside it. The base path was
	// resolved at startup, so without symlinks below it, the object
	// directory is inside it. Only resolve the whole path if there is one.
	if !fs.hasSymlinkBelowBase(objPath) {
		return nil
	}
	realObjPath, err := filepath.EvalSymlinks(objPath)
	if err != nil {
		return fmt.Errorf("resolving object path: %w", err)
	}
	if !strings.HasPrefix(realObjPath, fs.realBase+string(filepath.Separator)) {
		_ = os.RemoveAll(objPath) // Clean up potentially dangerous directory
		return fmt.Errorf("%w: path escapes base directory via symlink", ErrInvalidKey)
	}
	return nil
}

// maxDirsBelowBase is the number of directories from the base path down to
// an object directory: buckets/<bucket>/objects/<shard>/<object>
const maxDirsBelowBase = 5

// hasSymlinkBelowBase reports whether any directory between the base path
// and objPath, including objPath, is a symlink. It lstats only those
// directories, where filepath.EvalSymlinks lstats every component of the
// path from the root. It returns true when it cannot tell, so that the
// caller resolves the path in full.
func (fs *FilesystemStorage) hasSymlinkBelowBase(objPath string) bool {
	dir := objPath
	for range maxDirsBelowBase {
		info, err := os.Lstat(dir)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return true
		}
		dir = filepath.Dir(dir)
		if dir == fs.cleanBase {
			return false
		}
	}
	return true
}

// createObjectDir creates an object directory and its shard directory inside
// the bucket's objects directory. Unlike os.MkdirAll it never creates the
// objects directory itself, so it returns ErrBucketNotFound instead of
// reviving a bucket that was deleted concurrently.
func createObjectDir(objPath string) error {
	shardPath := filepath.Dir(objPath)
	var err error
	for range 3 {
		// Only DeleteBucket removes the objects directory, so failing to
		// create a shard directory inside it means the bucket is gone or
		// being deleted.
		if err = os.Mkdir(shardPath, 0700); err != nil && !os.IsExist(err) {
			if isMissingDirError(err) {
				return ErrBucketNotFound
			}
			return fmt.Errorf("creating shard directory: %w", err)
		}
		// DeleteObject removes empty shard directories, so the shard can
		// disappear before the object directory is created in it. Retry a
		// few times when it does.
		if err = os.Mkdir(objPath, 0700); err == nil || os.IsExist(err) {
			return nil
		}
		if !isMissingDirError(err) {
			break
		}
	}
	return fmt.Errorf("creating object directory: %w", err)
}

// isMissingDirError reports whether a Mkdir failed because its parent
// directory does not exist or is being removed. Creating a directory inside
// one that is being removed fails with ENOENT on Linux and EINVAL on macOS.
// On Linux, the removed directory can still be found by os.Stat for a short
// time after Mkdir fails, so the error is the reliable signal.
func isMissingDirError(err error) bool {
	return os.IsNotExist(err) || errors.Is(err, syscall.EINVAL)
}

// isNotEmptyError reports whether removing a directory failed because it
// is not empty
func isNotEmptyError(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}

// removeLeftoverObjectDirs removes empty object directories, and shard
// directories left empty, from a bucket's objects directory. It stops and
// returns true at the first object it finds (a directory with meta.json),
// so for a bucket with objects it reads little. The caller must hold the
// bucket's write lock, so that no write is between creating its object
// directory and renaming its files into it.
func removeLeftoverObjectDirs(objectsPath string) (hasObjects bool) {
	shards, err := os.ReadDir(objectsPath)
	if err != nil {
		return false
	}
	for _, shard := range shards {
		shardPath := filepath.Join(objectsPath, shard.Name())
		objects, err := os.ReadDir(shardPath)
		if err != nil {
			continue
		}
		for _, obj := range objects {
			objPath := filepath.Join(shardPath, obj.Name())
			if _, err := os.Stat(filepath.Join(objPath, "meta.json")); err == nil {
				return true
			}
			// Fails, and keeps the directory, unless it is empty
			_ = os.Remove(objPath)
		}
		// Fails unless the shard directory is now empty
		_ = os.Remove(shardPath)
	}
	return false
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

	// Remove the objects directory first. Unlike checking for entries and
	// then removing the tree, os.Remove fails atomically if anything is in
	// the directory, including an object created concurrently. Writers create
	// object directories without recreating objects/ (createObjectDir), so a
	// write that races with the removal fails instead of reviving the bucket.
	// A bucket with writes in progress is not empty. Writes that start
	// while it is being deleted wait, and then find it gone.
	bucketLock := fs.bucketLock(name)
	if !bucketLock.TryLock() {
		return ErrBucketNotEmpty
	}
	defer bucketLock.Unlock()

	err := os.Remove(objectsPath)
	if isNotEmptyError(err) {
		// The bucket may hold only empty object directories, left by failed
		// writes of earlier versions. With no writes in progress, an empty
		// object directory cannot belong to a write about to commit, so
		// remove them and try again.
		if hasObjects := removeLeftoverObjectDirs(objectsPath); hasObjects {
			return ErrBucketNotEmpty
		}
		err = os.Remove(objectsPath)
	}
	if err != nil {
		if isNotEmptyError(err) {
			return ErrBucketNotEmpty
		}
		if os.IsNotExist(err) {
			return ErrBucketNotFound
		}
		return fmt.Errorf("removing objects directory: %w", err)
	}
	// Without the objects directory, and with no writes in progress, the
	// rest of the bucket directory is leftover temp files
	if err := os.RemoveAll(bucketPath); err != nil {
		return fmt.Errorf("removing bucket directory: %w", err)
	}
	fs.dropIndex(name)

	return nil
}

// PutObject stores an object with the given key
func (fs *FilesystemStorage) PutObject(bucket, key string, contentType string, metadata map[string]string, body io.Reader) (*s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, err
	}

	// Keep the bucket from being deleted until the write is committed
	bucketLock := fs.bucketLock(bucket)
	bucketLock.RLock()
	defer bucketLock.RUnlock()

	// Write data to a temp file first, then rename. Each write has its own
	// temp files, so concurrent writes to the same key do not conflict.
	// The object directory is only created once the data has been received
	// (prepareObjectDir), so a failed upload leaves nothing behind.
	tmpPath, metaTmpPath, err := fs.newTempFilePaths(bucket)
	if err != nil {
		return nil, err
	}
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	// Calculate SHA-256 while writing
	hash := sha256.New()
	writer := io.MultiWriter(tmpFile, hash)

	size, err := copyBuffered(writer, body)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("writing object data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("closing temp file: %w", err)
	}

	// Create metadata
	now := time.Now().UTC()

	sum := hash.Sum(nil)
	objMeta := &s3.ObjectMetadata{
		Key:            key,
		Size:           size,
		ContentType:    contentType,
		ETag:           makeETag(sum, 0),
		LastModified:   now,
		UserMetadata:   metadata,
		ChecksumSHA256: EncodeChecksum(sum),
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
	if err := fs.commitObject(bucket, objPath, tmpPath, metaTmpPath, objMeta); err != nil {
		return nil, err
	}

	return objMeta, nil
}

// GetObject retrieves an object by key
func (fs *FilesystemStorage) GetObject(bucket, key string) (io.ReadSeekCloser, *s3.ObjectMetadata, error) {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return nil, nil, err
	}
	return fs.openObject(objPath)
}

// openObject opens an object's data file and reads its metadata under the
// object's commit lock, so that both come from the same write even when the
// object is being overwritten. The lock is only held while opening; the
// returned file keeps the data of that write while it is read.
func (fs *FilesystemStorage) openObject(objPath string) (*os.File, *s3.ObjectMetadata, error) {
	commit := fs.commitLock(objPath)
	commit.RLock()
	defer commit.RUnlock()

	meta, err := readObjectMetadata(filepath.Join(objPath, "meta.json"))
	if err != nil {
		return nil, nil, err
	}

	file, err := os.Open(filepath.Join(objPath, "data"))
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
	return readObjectMetadata(filepath.Join(objPath, "meta.json"))
}

// readObjectMetadata reads an object's meta.json
func readObjectMetadata(metaPath string) (*s3.ObjectMetadata, error) {
	var meta s3.ObjectMetadata
	if err := readJSONFile(metaPath, &meta); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("reading metadata: %w", err)
	}
	return &meta, nil
}

// commitObject moves an object's data and metadata temp files into place.
// Both renames happen under the object's commit lock, so concurrent writes
// to the same key cannot interleave and leave the data of one write with
// the metadata of another. If the metadata cannot be renamed, the data file
// is removed as well.
func (fs *FilesystemStorage) commitObject(bucket, objPath, dataTmpPath, metaTmpPath string, meta *s3.ObjectMetadata) error {
	commit := fs.commitLock(objPath)
	commit.Lock()
	defer commit.Unlock()

	dataPath := filepath.Join(objPath, "data")
	if err := os.Rename(dataTmpPath, dataPath); err != nil {
		_ = os.Remove(dataTmpPath)
		_ = os.Remove(metaTmpPath)
		return fmt.Errorf("renaming data file: %w", err)
	}
	if err := os.Rename(metaTmpPath, filepath.Join(objPath, "meta.json")); err != nil {
		_ = os.Remove(metaTmpPath)
		_ = os.Remove(dataPath)
		return fmt.Errorf("renaming metadata file: %w", err)
	}
	if idx := fs.existingIndex(bucket); idx != nil {
		idx.set(meta)
	}
	return nil
}

// DeleteObject removes an object by key
func (fs *FilesystemStorage) DeleteObject(bucket, key string) error {
	objPath, err := fs.keyToPath(bucket, key)
	if err != nil {
		return err
	}

	// Remove the entire object directory. The commit lock keeps the removal
	// from landing between the data and metadata renames of a write.
	commit := fs.commitLock(objPath)
	commit.Lock()
	err = removeObjectDir(objPath)
	if err == nil {
		if idx := fs.existingIndex(bucket); idx != nil {
			idx.remove(key)
		}
	}
	commit.Unlock()
	if err != nil {
		return fmt.Errorf("removing object: %w", err)
	}

	// Try to remove empty parent directory (ignore errors)
	parentDir := filepath.Dir(objPath)
	_ = syscall.Rmdir(parentDir) // only succeeds if empty

	return nil
}

// removeObjectDir removes an object directory. An object directory holds
// only meta.json and data, so it removes those and then the directory: three
// system calls, where os.RemoveAll first tries to remove the directory as a
// file and as a directory, and then opens it and reads its entries.
// meta.json goes first, so the object stops being found before its data
// is gone. A directory that is gone already counts as removed, and one that
// still holds anything else is left to os.RemoveAll.
func removeObjectDir(objPath string) error {
	_ = syscall.Unlink(filepath.Join(objPath, "meta.json"))
	_ = syscall.Unlink(filepath.Join(objPath, "data"))
	err := syscall.Rmdir(objPath)
	if err == nil || errors.Is(err, syscall.ENOENT) {
		return nil
	}
	return os.RemoveAll(objPath)
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
	file, meta, err := fs.openObject(objPath)
	if err != nil {
		return nil, nil, err
	}

	// Handle empty files - no valid range exists
	if meta.Size == 0 {
		_ = file.Close()
		return nil, nil, fmt.Errorf("invalid range: object is empty")
	}

	// Validate range
	if start < 0 {
		start = 0
	}
	if end < 0 || end >= meta.Size {
		end = meta.Size - 1
	}
	if start > end {
		_ = file.Close()
		return nil, nil, fmt.Errorf("invalid range: start > end")
	}

	// Seek to start position
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		_ = file.Close()
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

// listTokenPrefixMarker starts the continuation token of a page that ended
// with a common prefix. Keys cannot contain a NUL byte (ValidateKey), so it
// cannot be confused with the token of a page that ended with an object.
const listTokenPrefixMarker = "\x00"

// listToken returns the continuation token for a listing page that ended
// with the given key or, if isPrefix, common prefix. The next page starts
// after the key, or after every key under the prefix.
func listToken(keyOrPrefix string, isPrefix bool) string {
	if isPrefix {
		keyOrPrefix = listTokenPrefixMarker + keyOrPrefix
	}
	return base64.URLEncoding.EncodeToString([]byte(keyOrPrefix))
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

	idx, err := fs.loadIndex(bucket)
	if err != nil {
		return nil, err
	}

	result := &ListObjectsResult{}
	startKey := opts.StartAfter
	skipPrefix := ""
	if opts.ContinuationToken != "" {
		// Decode continuation token (see listToken)
		decoded, err := base64.URLEncoding.DecodeString(opts.ContinuationToken)
		if err == nil {
			startKey = string(decoded)
			if p, ok := strings.CutPrefix(startKey, listTokenPrefixMarker); ok {
				startKey, skipPrefix = p, p
			}
		}
	}

	// Keys under opts.Prefix form one range of the sorted index, starting at
	// the prefix itself. cursor is where the scan (re)starts: past startKey,
	// and past every key under a common prefix that ended the previous page.
	cursor := opts.Prefix
	if startKey > cursor {
		cursor = startKey
	}
	if skipPrefix != "" {
		next, ok := prefixSuccessor(skipPrefix)
		if !ok {
			return result, nil
		}
		if next > cursor {
			cursor = next
		}
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Each object and each common prefix counts as one entry against
	// MaxKeys, as in S3. last is the last entry, from which the
	// continuation token is made if the page is truncated. After a common
	// prefix, the scan restarts past all keys under it.
	count := 0
	last, lastIsPrefix := "", false
	truncate := func() bool {
		result.IsTruncated = true
		result.NextContinuationToken = listToken(last, lastIsPrefix)
		return false
	}
	for done := false; !done; {
		done = true
		idx.tree.AscendGreaterOrEqual(&indexEntry{key: cursor}, func(e *indexEntry) bool {
			if startKey != "" && e.key <= startKey {
				return true
			}
			if !strings.HasPrefix(e.key, opts.Prefix) {
				return false
			}
			if opts.Delimiter != "" {
				afterPrefix := e.key[len(opts.Prefix):]
				if delimIdx := strings.Index(afterPrefix, opts.Delimiter); delimIdx >= 0 {
					commonPrefix := opts.Prefix + afterPrefix[:delimIdx+len(opts.Delimiter)]
					if count >= opts.MaxKeys {
						return truncate()
					}
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
					count++
					last, lastIsPrefix = commonPrefix, true
					if next, ok := prefixSuccessor(commonPrefix); ok {
						cursor, done = next, false
					}
					return false
				}
			}
			if count >= opts.MaxKeys {
				return truncate()
			}
			result.Objects = append(result.Objects, e.metadata())
			count++
			last, lastIsPrefix = e.key, false
			return true
		})
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
	defer func() { _ = srcReader.Close() }()

	// Copy to destination
	dstMeta, err := fs.PutObject(dstBucket, dstKey, srcMeta.ContentType, srcMeta.UserMetadata, srcReader)
	if err != nil {
		return nil, fmt.Errorf("copying object: %w", err)
	}

	return dstMeta, nil
}

// copyBufferSize is the size of the buffers in copyBufferPool, the same as
// io.Copy uses
const copyBufferSize = 32 * 1024

// copyBufferPool holds the buffers that request bodies are copied to disk
// through. A request body is not an io.WriterTo and the destination, which
// also hashes the data, is not an io.ReaderFrom, so io.Copy would allocate
// a new buffer for every upload.
var copyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, copyBufferSize)
		return &buf
	},
}

// copyBuffered is io.Copy with a buffer from copyBufferPool
func copyBuffered(dst io.Writer, src io.Reader) (int64, error) {
	buf := copyBufferPool.Get().(*[]byte)
	defer copyBufferPool.Put(buf)
	return io.CopyBuffer(dst, src, *buf)
}

// getUploadLock returns the per-upload RWMutex for uploadID, creating it if needed.
func (fs *FilesystemStorage) getUploadLock(uploadID string) *sync.RWMutex {
	v, _ := fs.uploadLocks.LoadOrStore(uploadID, &sync.RWMutex{})
	return v.(*sync.RWMutex)
}

// Helper functions
