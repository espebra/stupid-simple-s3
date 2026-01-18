package api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// Handlers contains all S3 API handlers
type Handlers struct {
	cfg     *config.Config
	storage storage.MultipartStorage
}

// NewHandlers creates a new Handlers instance
func NewHandlers(cfg *config.Config, store storage.MultipartStorage) *Handlers {
	return &Handlers{
		cfg:     cfg,
		storage: store,
	}
}

// HeadBucket handles HEAD /{bucket}
func (h *Handlers) HeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PutObject handles PUT /{bucket}/{key...}
func (h *Handlers) PutObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	// Check for multipart upload operations
	query := r.URL.Query()
	if query.Has("uploadId") && query.Has("partNumber") {
		h.UploadPart(w, r)
		return
	}

	// Check for copy operation
	copySource := r.Header.Get("X-Amz-Copy-Source")
	if copySource != "" {
		h.CopyObject(w, r)
		return
	}

	// Regular put object
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract user metadata (x-amz-meta-* headers)
	userMetadata := make(map[string]string)
	for name, values := range r.Header {
		lowerName := strings.ToLower(name)
		if strings.HasPrefix(lowerName, "x-amz-meta-") && len(values) > 0 {
			metaKey := strings.TrimPrefix(lowerName, "x-amz-meta-")
			userMetadata[metaKey] = values[0]
		}
	}

	// Handle AWS chunked encoding (used by Minio SDK and some AWS SDK configurations)
	body := wrapBodyIfChunked(r.Body, r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256"))

	meta, err := h.storage.PutObject(key, contentType, userMetadata, body)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

// CopyObject handles PUT /{bucket}/{key} with X-Amz-Copy-Source header
func (h *Handlers) CopyObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	dstKey := r.PathValue("key")

	copySource := r.Header.Get("X-Amz-Copy-Source")

	// Parse copy source: /bucket/key or bucket/key (URL encoded)
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")

	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+dstKey).WriteResponse(w)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Only support copying within the same bucket
	if srcBucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+srcBucket).WriteResponse(w)
		return
	}

	// Copy the object
	meta, err := h.storage.CopyObject(srcKey, dstKey)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.NewError(s3.ErrNoSuchKey, "/"+srcBucket+"/"+srcKey).WriteResponse(w)
			return
		}
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+dstKey).WriteResponse(w)
		return
	}

	result := s3.CopyObjectResult{
		ETag:         meta.ETag,
		LastModified: meta.LastModified,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(result)
}

// GetObject handles GET /{bucket}/{key...}
func (h *Handlers) GetObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	// Check for Range header
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		h.GetObjectRange(w, r)
		return
	}

	reader, meta, err := h.storage.GetObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.NewError(s3.ErrNoSuchKey, "/"+bucket+"/"+key).WriteResponse(w)
			return
		}
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}
	defer reader.Close()

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	// Set user metadata headers
	for k, v := range meta.UserMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

// GetObjectRange handles GET with Range header
func (h *Handlers) GetObjectRange(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	rangeHeader := r.Header.Get("Range")

	// Parse range header: bytes=start-end or bytes=start- or bytes=-suffix
	start, end, err := parseRangeHeader(rangeHeader)
	if err != nil {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Get object metadata first to validate range
	meta, err := h.storage.HeadObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.NewError(s3.ErrNoSuchKey, "/"+bucket+"/"+key).WriteResponse(w)
			return
		}
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Handle suffix range (bytes=-N means last N bytes)
	if start < 0 {
		start = meta.Size + start
		if start < 0 {
			start = 0
		}
		end = meta.Size - 1
	}

	// Handle open-ended range (bytes=N-)
	if end < 0 || end >= meta.Size {
		end = meta.Size - 1
	}

	// Validate range
	if start > end || start >= meta.Size {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	reader, _, err := h.storage.GetObjectRange(key, start, end)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}
	defer reader.Close()

	contentLength := end - start + 1

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set("Accept-Ranges", "bytes")

	// Set user metadata headers
	for k, v := range meta.UserMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.Copy(w, reader)
}

// parseRangeHeader parses a Range header value
// Returns start, end (-1 means unspecified)
func parseRangeHeader(rangeHeader string) (start, end int64, err error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	// Parse start
	if parts[0] == "" {
		// Suffix range: -N
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range end")
		}
		return -end, -1, nil
	}

	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range start")
	}

	// Parse end
	if parts[1] == "" {
		// Open-ended range: N-
		return start, -1, nil
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range end")
	}

	return start, end, nil
}

// HeadObject handles HEAD /{bucket}/{key...}
func (h *Handlers) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	meta, err := h.storage.HeadObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.NewError(s3.ErrNoSuchKey, "/"+bucket+"/"+key).WriteResponse(w)
			return
		}
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Set response headers
	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))

	// Set user metadata headers
	for k, v := range meta.UserMetadata {
		w.Header().Set("x-amz-meta-"+k, v)
	}

	w.WriteHeader(http.StatusOK)
}

// DeleteObject handles DELETE /{bucket}/{key...}
func (h *Handlers) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	// Check for multipart upload abort
	query := r.URL.Query()
	if query.Has("uploadId") {
		h.AbortMultipartUpload(w, r)
		return
	}

	err := h.storage.DeleteObject(key)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// CreateMultipartUpload handles POST /{bucket}/{key}?uploads
func (h *Handlers) CreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract user metadata
	userMetadata := make(map[string]string)
	for name, values := range r.Header {
		lowerName := strings.ToLower(name)
		if strings.HasPrefix(lowerName, "x-amz-meta-") && len(values) > 0 {
			metaKey := strings.TrimPrefix(lowerName, "x-amz-meta-")
			userMetadata[metaKey] = values[0]
		}
	}

	uploadID, err := h.storage.CreateMultipartUpload(key, contentType, userMetadata)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	result := s3.InitiateMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucket,
		Key:      key,
		UploadID: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(result)
}

// UploadPart handles PUT /{bucket}/{key}?partNumber=N&uploadId=X
func (h *Handlers) UploadPart(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")
	partNumberStr := query.Get("partNumber")

	if uploadID == "" || partNumberStr == "" {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Verify upload exists and key matches
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.NewError(s3.ErrNoSuchUpload, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	if uploadMeta.Key != key {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Handle AWS chunked encoding
	body := wrapBodyIfChunked(r.Body, r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256"))

	partMeta, err := h.storage.UploadPart(uploadID, partNumber, body)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	w.Header().Set("ETag", partMeta.ETag)
	w.WriteHeader(http.StatusOK)
}

// CompleteMultipartUpload handles POST /{bucket}/{key}?uploadId=X
func (h *Handlers) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")

	if uploadID == "" {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Verify upload exists and key matches
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.NewError(s3.ErrNoSuchUpload, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	if uploadMeta.Key != key {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Parse request body
	var completeReq s3.CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&completeReq); err != nil {
		s3.NewError(s3.ErrMalformedXML, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Complete the upload
	objMeta, err := h.storage.CompleteMultipartUpload(uploadID, completeReq.Parts)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.NewError(s3.ErrInvalidPart, "/"+bucket+"/"+key).WriteResponse(w)
			return
		}
		if strings.Contains(err.Error(), "order") {
			s3.NewError(s3.ErrInvalidPartOrder, "/"+bucket+"/"+key).WriteResponse(w)
			return
		}
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	result := s3.CompleteMultipartUploadResult{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Location: "http://" + r.Host + "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     objMeta.ETag,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(result)
}

// AbortMultipartUpload handles DELETE /{bucket}/{key}?uploadId=X
func (h *Handlers) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")

	if uploadID == "" {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	// Verify upload exists
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.NewError(s3.ErrNoSuchUpload, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	if uploadMeta.Key != key {
		s3.NewError(s3.ErrInvalidArgument, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	if err := h.storage.AbortMultipartUpload(uploadID); err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket+"/"+key).WriteResponse(w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// PostObject handles POST requests to /{bucket}/{key...}
// Routes to either CreateMultipartUpload or CompleteMultipartUpload
func (h *Handlers) PostObject(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	if query.Has("uploads") {
		h.CreateMultipartUpload(w, r)
		return
	}

	if query.Has("uploadId") {
		h.CompleteMultipartUpload(w, r)
		return
	}

	s3.NewError(s3.ErrInvalidRequest, r.URL.Path).WriteResponse(w)
}

// GetBucket handles GET /{bucket} for listing objects or bucket operations
func (h *Handlers) GetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	query := r.URL.Query()

	// ListObjectsV2 (list-type=2) or ListObjects (no list-type)
	if query.Get("list-type") == "2" {
		h.ListObjectsV2(w, r)
		return
	}

	// Default to ListObjectsV2 behavior for simplicity
	h.ListObjectsV2(w, r)
}

// ListObjectsV2 handles GET /{bucket}?list-type=2
func (h *Handlers) ListObjectsV2(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	query := r.URL.Query()

	maxKeys := 1000
	if maxKeysStr := query.Get("max-keys"); maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	opts := storage.ListObjectsOptions{
		Prefix:            query.Get("prefix"),
		Delimiter:         query.Get("delimiter"),
		MaxKeys:           maxKeys,
		StartAfter:        query.Get("start-after"),
		ContinuationToken: query.Get("continuation-token"),
	}

	result, err := h.storage.ListObjects(opts)
	if err != nil {
		s3.NewError(s3.ErrInternalError, "/"+bucket).WriteResponse(w)
		return
	}

	// Build response
	var objects []s3.Object
	for _, obj := range result.Objects {
		objects = append(objects, s3.Object{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: "STANDARD",
		})
	}

	var commonPrefixes []s3.Prefix
	for _, prefix := range result.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, s3.Prefix{Prefix: prefix})
	}

	response := s3.ListBucketResultV2{
		Xmlns:                 "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:                  bucket,
		Prefix:                opts.Prefix,
		Delimiter:             opts.Delimiter,
		MaxKeys:               maxKeys,
		KeyCount:              len(objects),
		IsTruncated:           result.IsTruncated,
		StartAfter:            opts.StartAfter,
		ContinuationToken:     opts.ContinuationToken,
		NextContinuationToken: result.NextContinuationToken,
		Contents:              objects,
		CommonPrefixes:        commonPrefixes,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(response)
}

// PostBucket handles POST /{bucket} for bucket-level operations like DeleteObjects
func (h *Handlers) PostBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")

	if bucket != h.cfg.Bucket.Name {
		s3.NewError(s3.ErrNoSuchBucket, "/"+bucket).WriteResponse(w)
		return
	}

	query := r.URL.Query()

	if query.Has("delete") {
		h.DeleteObjects(w, r)
		return
	}

	s3.NewError(s3.ErrInvalidRequest, "/"+bucket).WriteResponse(w)
}

// DeleteObjects handles POST /{bucket}?delete (batch delete)
func (h *Handlers) DeleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")

	// Parse request body
	var deleteReq s3.Delete
	if err := xml.NewDecoder(r.Body).Decode(&deleteReq); err != nil {
		s3.NewError(s3.ErrMalformedXML, "/"+bucket).WriteResponse(w)
		return
	}

	result := s3.DeleteObjectsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}

	for _, obj := range deleteReq.Objects {
		err := h.storage.DeleteObject(obj.Key)
		if err != nil {
			result.Error = append(result.Error, s3.DeleteError{
				Key:     obj.Key,
				Code:    string(s3.ErrInternalError),
				Message: "Failed to delete object",
			})
		} else if !deleteReq.Quiet {
			result.Deleted = append(result.Deleted, s3.DeletedObject{
				Key: obj.Key,
			})
		}
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_ = xml.NewEncoder(w).Encode(result)
}
