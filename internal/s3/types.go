package s3

import (
	"encoding/xml"
	"time"
)

// InitiateMultipartUploadResult is the response for CreateMultipartUpload
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUpload is the request body for CompleteMultipartUpload
type CompleteMultipartUpload struct {
	XMLName xml.Name             `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPartInput `xml:"Part"`
}

// CompletedPartInput represents a part in the CompleteMultipartUpload request
type CompletedPartInput struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResult is the response for CompleteMultipartUpload
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// CopyObjectResult is the response for CopyObject
type CopyObjectResult struct {
	XMLName      xml.Name  `xml:"CopyObjectResult"`
	ETag         string    `xml:"ETag"`
	LastModified time.Time `xml:"LastModified"`
}

// DeleteResult is the response for DeleteObject
type DeleteResult struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// ListBucketResult is the response for ListObjects (v1)
type ListBucketResult struct {
	XMLName        xml.Name `xml:"ListBucketResult"`
	Xmlns          string   `xml:"xmlns,attr"`
	Name           string   `xml:"Name"`
	Prefix         string   `xml:"Prefix"`
	Marker         string   `xml:"Marker"`
	MaxKeys        int      `xml:"MaxKeys"`
	IsTruncated    bool     `xml:"IsTruncated"`
	Contents       []Object `xml:"Contents"`
	CommonPrefixes []Prefix `xml:"CommonPrefixes,omitempty"`
}

// ListBucketResultV2 is the response for ListObjectsV2
type ListBucketResultV2 struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	Xmlns                 string   `xml:"xmlns,attr"`
	Name                  string   `xml:"Name"`
	Prefix                string   `xml:"Prefix,omitempty"`
	StartAfter            string   `xml:"StartAfter,omitempty"`
	KeyCount              int      `xml:"KeyCount"`
	MaxKeys               int      `xml:"MaxKeys"`
	Delimiter             string   `xml:"Delimiter,omitempty"`
	IsTruncated           bool     `xml:"IsTruncated"`
	ContinuationToken     string   `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
	Contents              []Object `xml:"Contents"`
	CommonPrefixes        []Prefix `xml:"CommonPrefixes,omitempty"`
}

// Delete is the request body for DeleteObjects
type Delete struct {
	XMLName xml.Name         `xml:"Delete"`
	Quiet   bool             `xml:"Quiet"`
	Objects []ObjectToDelete `xml:"Object"`
}

// ObjectToDelete represents an object to delete in DeleteObjects request
type ObjectToDelete struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// DeleteResult is the response for DeleteObjects
type DeleteObjectsResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []DeletedObject `xml:"Deleted,omitempty"`
	Error   []DeleteError   `xml:"Error,omitempty"`
}

// DeletedObject represents a successfully deleted object
type DeletedObject struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
}

// DeleteError represents an error deleting an object
type DeleteError struct {
	Key       string `xml:"Key"`
	VersionId string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}

// Object represents an S3 object in list responses
type Object struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

// Prefix represents a common prefix in list responses
type Prefix struct {
	Prefix string `xml:"Prefix"`
}

// ObjectMetadata stores object metadata internally
type ObjectMetadata struct {
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ContentType  string            `json:"content_type"`
	ETag         string            `json:"etag"`
	LastModified time.Time         `json:"last_modified"`
	UserMetadata map[string]string `json:"user_metadata,omitempty"`
}

// MultipartUploadMetadata stores multipart upload metadata
type MultipartUploadMetadata struct {
	UploadID     string            `json:"upload_id"`
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	Created      time.Time         `json:"created"`
	ContentType  string            `json:"content_type,omitempty"`
	UserMetadata map[string]string `json:"user_metadata,omitempty"`
}

// PartMetadata stores information about an uploaded part
type PartMetadata struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	Size       int64  `json:"size"`
}
