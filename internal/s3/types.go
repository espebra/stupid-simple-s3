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

// LocationConstraint is the response for GetBucketLocation. An empty value
// means us-east-1.
type LocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr"`
	Value   string   `xml:",chardata"`
}

// ListMultipartUploadsResult is the response for ListMultipartUploads
type ListMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns              string            `xml:"xmlns,attr"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIDMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker,omitempty"`
	Prefix             string            `xml:"Prefix"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Uploads            []MultipartUpload `xml:"Upload"`
}

// MultipartUpload describes an in-progress upload in ListMultipartUploadsResult
type MultipartUpload struct {
	Key          string    `xml:"Key"`
	UploadID     string    `xml:"UploadId"`
	StorageClass string    `xml:"StorageClass"`
	Initiated    time.Time `xml:"Initiated"`
}

// ListPartsResult is the response for ListParts
type ListPartsResult struct {
	XMLName              xml.Name `xml:"ListPartsResult"`
	Xmlns                string   `xml:"xmlns,attr"`
	Bucket               string   `xml:"Bucket"`
	Key                  string   `xml:"Key"`
	UploadID             string   `xml:"UploadId"`
	StorageClass         string   `xml:"StorageClass"`
	PartNumberMarker     int      `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int      `xml:"MaxParts"`
	IsTruncated          bool     `xml:"IsTruncated"`
	Parts                []Part   `xml:"Part"`
}

// Part describes an uploaded part in ListPartsResult
type Part struct {
	PartNumber     int       `xml:"PartNumber"`
	LastModified   time.Time `xml:"LastModified"`
	ETag           string    `xml:"ETag"`
	Size           int64     `xml:"Size"`
	ChecksumSHA256 string    `xml:"ChecksumSHA256,omitempty"`
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
	Location string   `xml:"Location,omitempty"` // Optional - omit to avoid host header injection
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
	// ChecksumSHA256 is the composite SHA-256 of the object, with a "-N" suffix
	ChecksumSHA256 string `xml:"ChecksumSHA256,omitempty"`
	ChecksumType   string `xml:"ChecksumType,omitempty"`
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
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ContentType  string    `json:"content_type"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	// ChecksumSHA256 is the base64-encoded SHA-256 of the object data, as sent
	// in the x-amz-checksum-sha256 header. Multipart objects have a composite
	// checksum: the SHA-256 of the concatenated part digests with a "-N" suffix.
	// Empty for objects stored before SHA-256 checksums were introduced.
	ChecksumSHA256 string            `json:"checksum_sha256,omitempty"`
	UserMetadata   map[string]string `json:"user_metadata,omitempty"`
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
	// ChecksumSHA256 is the base64-encoded SHA-256 of the part data
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
	// LastModified is when the part was uploaded. It is not stored in the
	// metadata file; ListParts fills it in from the part file.
	LastModified time.Time `json:"-"`
}
