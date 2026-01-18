package storage

import (
	"io"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// ListObjectsOptions contains options for listing objects
type ListObjectsOptions struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int
	StartAfter        string
	ContinuationToken string
}

// ListObjectsResult contains the result of listing objects
type ListObjectsResult struct {
	Objects               []s3.ObjectMetadata
	CommonPrefixes        []string
	IsTruncated           bool
	NextContinuationToken string
}

// Storage defines the interface for object storage operations
type Storage interface {
	// PutObject stores an object with the given key
	PutObject(key string, contentType string, metadata map[string]string, body io.Reader) (*s3.ObjectMetadata, error)

	// GetObject retrieves an object by key
	GetObject(key string) (io.ReadCloser, *s3.ObjectMetadata, error)

	// GetObjectRange retrieves a range of bytes from an object
	GetObjectRange(key string, start, end int64) (io.ReadCloser, *s3.ObjectMetadata, error)

	// HeadObject retrieves object metadata without the body
	HeadObject(key string) (*s3.ObjectMetadata, error)

	// DeleteObject removes an object by key
	DeleteObject(key string) error

	// ObjectExists checks if an object exists
	ObjectExists(key string) (bool, error)

	// ListObjects lists objects with optional prefix, delimiter, and pagination
	ListObjects(opts ListObjectsOptions) (*ListObjectsResult, error)

	// CopyObject copies an object from source key to destination key
	CopyObject(srcKey, dstKey string) (*s3.ObjectMetadata, error)
}

// MultipartStorage defines the interface for multipart upload operations
type MultipartStorage interface {
	Storage

	// CreateMultipartUpload initializes a new multipart upload
	CreateMultipartUpload(key string, contentType string, metadata map[string]string) (uploadID string, err error)

	// UploadPart stores a part of a multipart upload
	UploadPart(uploadID string, partNumber int, body io.Reader) (*s3.PartMetadata, error)

	// CompleteMultipartUpload assembles all parts into the final object
	CompleteMultipartUpload(uploadID string, parts []s3.CompletedPartInput) (*s3.ObjectMetadata, error)

	// AbortMultipartUpload cancels a multipart upload and cleans up parts
	AbortMultipartUpload(uploadID string) error

	// GetMultipartUpload retrieves metadata about a multipart upload
	GetMultipartUpload(uploadID string) (*s3.MultipartUploadMetadata, error)
}
