package api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/espen/stupid-simple-s3/internal/auth"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/metrics"
	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// Maximum allowed value for max-keys parameter
const maxKeysLimit = 1000

// ErrInvalidMetadata is returned when metadata contains invalid characters
var ErrInvalidMetadata = errors.New("invalid metadata")

// validateMetadataValue checks that a metadata value doesn't contain characters
// that could be used for header injection attacks (CRLF injection).
func validateMetadataValue(value string) error {
	if strings.ContainsAny(value, "\r\n\x00") {
		return ErrInvalidMetadata
	}
	return nil
}

// validateMetadataKey checks that a metadata key is valid.
func validateMetadataKey(key string) error {
	if strings.ContainsAny(key, "\r\n\x00") {
		return ErrInvalidMetadata
	}
	return nil
}

// extractAndValidateMetadata extracts x-amz-meta-* headers and validates them.
// Returns the metadata map and an error if validation fails.
func extractAndValidateMetadata(headers http.Header) (map[string]string, error) {
	userMetadata := make(map[string]string)
	for name, values := range headers {
		lowerName := strings.ToLower(name)
		if strings.HasPrefix(lowerName, "x-amz-meta-") && len(values) > 0 {
			metaKey := strings.TrimPrefix(lowerName, "x-amz-meta-")
			if err := validateMetadataKey(metaKey); err != nil {
				return nil, err
			}
			if err := validateMetadataValue(values[0]); err != nil {
				return nil, err
			}
			userMetadata[metaKey] = values[0]
		}
	}
	return userMetadata, nil
}

// limitedReader wraps an io.Reader to enforce a maximum read size.
// Returns an error when the limit is exceeded.
type limitedReader struct {
	r         io.Reader
	remaining int64
}

func newLimitedReader(r io.Reader, limit int64) *limitedReader {
	return &limitedReader{r: r, remaining: limit}
}

func (l *limitedReader) Read(p []byte) (n int, err error) {
	if l.remaining <= 0 {
		// Try to read one more byte to see if there's more data
		var buf [1]byte
		n, err := l.r.Read(buf[:])
		if n > 0 {
			return 0, errors.New("entity too large")
		}
		return 0, err
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err = l.r.Read(p)
	l.remaining -= int64(n)
	return n, err
}

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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PutObject handles PUT /{bucket}/{key...}
func (h *Handlers) PutObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
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

	// Track active upload
	metrics.UploadsActive.Inc()
	defer metrics.UploadsActive.Dec()

	// Regular put object
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract and validate user metadata (x-amz-meta-* headers)
	userMetadata, err := extractAndValidateMetadata(r.Header)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Handle AWS chunked encoding (used by Minio SDK and some AWS SDK configurations)
	var body io.Reader = wrapBodyIfChunked(r.Body, r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256"))

	// Enforce maximum object size limit
	if h.cfg.Limits.MaxObjectSize > 0 {
		body = newLimitedReader(body, h.cfg.Limits.MaxObjectSize)
	}

	meta, err := h.storage.PutObject(key, contentType, userMetadata, body)
	if err != nil {
		if strings.Contains(err.Error(), "entity too large") {
			s3.WriteErrorResponse(w, s3.ErrEntityTooLarge)
			return
		}
		if strings.Contains(err.Error(), "invalid object key") {
			s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
		return
	}

	w.Header().Set("ETag", meta.ETag)
	w.WriteHeader(http.StatusOK)
}

// CopyObject handles PUT /{bucket}/{key} with X-Amz-Copy-Source header
func (h *Handlers) CopyObject(w http.ResponseWriter, r *http.Request) {
	dstBucket := r.PathValue("bucket")
	dstKey := r.PathValue("key")

	// Validate destination bucket
	if dstBucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	copySource := r.Header.Get("X-Amz-Copy-Source")

	// Parse copy source: /bucket/key or bucket/key (URL encoded)
	copySource, _ = url.PathUnescape(copySource)
	copySource = strings.TrimPrefix(copySource, "/")

	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) != 2 {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	srcBucket := parts[0]
	srcKey := parts[1]

	// Only support copying within the same bucket
	if srcBucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	// Copy the object
	meta, err := h.storage.CopyObject(srcKey, dstKey)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.WriteErrorResponse(w, s3.ErrNoSuchKey)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	// Check for Range header
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		h.GetObjectRange(w, r)
		return
	}

	// Track active download
	metrics.DownloadsActive.Inc()
	defer metrics.DownloadsActive.Dec()

	reader, meta, err := h.storage.GetObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.WriteErrorResponse(w, s3.ErrNoSuchKey)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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

	// Apply response header overrides for presigned URLs
	applyResponseHeaderOverrides(w, r)

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

// GetObjectRange handles GET with Range header
func (h *Handlers) GetObjectRange(w http.ResponseWriter, r *http.Request) {
	// Track active download
	metrics.DownloadsActive.Inc()
	defer metrics.DownloadsActive.Dec()

	// Note: bucket validation is done in GetObject before calling this handler
	_ = r.PathValue("bucket")
	key := r.PathValue("key")

	rangeHeader := r.Header.Get("Range")

	// Parse range header: bytes=start-end or bytes=start- or bytes=-suffix
	start, end, err := parseRangeHeader(rangeHeader)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Get object metadata first to validate range
	meta, err := h.storage.HeadObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.WriteErrorResponse(w, s3.ErrNoSuchKey)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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

	// Apply response header overrides for presigned URLs
	applyResponseHeaderOverrides(w, r)

	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.Copy(w, reader)
}

// applyResponseHeaderOverrides applies response header overrides from presigned URL query parameters.
// Only applies overrides for presigned requests.
// validateHeaderValue checks if a header value is safe (no CRLF injection)
func validateHeaderValue(value string) bool {
	return !strings.ContainsAny(value, "\r\n\x00")
}

func applyResponseHeaderOverrides(w http.ResponseWriter, r *http.Request) {
	// Only apply overrides for presigned requests
	if !auth.IsPresignedRequest(r) {
		return
	}

	query := r.URL.Query()

	// Validate all header values to prevent CRLF injection
	if v := query.Get("response-content-type"); v != "" && validateHeaderValue(v) {
		w.Header().Set("Content-Type", v)
	}
	if v := query.Get("response-content-disposition"); v != "" && validateHeaderValue(v) {
		w.Header().Set("Content-Disposition", v)
	}
	if v := query.Get("response-cache-control"); v != "" && validateHeaderValue(v) {
		w.Header().Set("Cache-Control", v)
	}
}

// parseRangeHeader parses a Range header value
// Returns start, end (-1 means unspecified)
// Validates that values are within safe bounds to prevent integer overflow
func parseRangeHeader(rangeHeader string) (start, end int64, err error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header: must start with 'bytes='")
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

	// Reject multiple ranges (not supported)
	if strings.Contains(rangeSpec, ",") {
		return 0, 0, fmt.Errorf("invalid range: multiple ranges not supported")
	}

	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format")
	}

	// Maximum safe value for range (to prevent overflow)
	const maxRangeValue = math.MaxInt64 / 2

	// Parse start
	if parts[0] == "" {
		// Suffix range: -N
		if parts[1] == "" {
			return 0, 0, fmt.Errorf("invalid range: both start and end are empty")
		}
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid range end: %w", err)
		}
		if end < 0 || end > maxRangeValue {
			return 0, 0, fmt.Errorf("invalid range: end value out of bounds")
		}
		return -end, -1, nil
	}

	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range start: %w", err)
	}
	if start < 0 || start > maxRangeValue {
		return 0, 0, fmt.Errorf("invalid range: start value out of bounds")
	}

	// Parse end
	if parts[1] == "" {
		// Open-ended range: N-
		return start, -1, nil
	}

	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range end: %w", err)
	}
	if end < 0 || end > maxRangeValue {
		return 0, 0, fmt.Errorf("invalid range: end value out of bounds")
	}

	// Validate start <= end
	if start > end {
		return 0, 0, fmt.Errorf("invalid range: start > end")
	}

	return start, end, nil
}

// HeadObject handles HEAD /{bucket}/{key...}
func (h *Handlers) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	meta, err := h.storage.HeadObject(key)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.WriteErrorResponse(w, s3.ErrNoSuchKey)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
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
		s3.WriteErrorResponse(w, s3.ErrInternalError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// CreateMultipartUpload handles POST /{bucket}/{key}?uploads
func (h *Handlers) CreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")

	if bucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Extract and validate user metadata
	userMetadata, err := extractAndValidateMetadata(r.Header)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	uploadID, err := h.storage.CreateMultipartUpload(key, contentType, userMetadata)
	if err != nil {
		if strings.Contains(err.Error(), "invalid object key") {
			s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")
	partNumberStr := query.Get("partNumber")

	if uploadID == "" || partNumberStr == "" {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	partNumber, err := strconv.Atoi(partNumberStr)
	if err != nil || partNumber < 1 || partNumber > 10000 {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Verify upload exists and key matches
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrNoSuchUpload)
		return
	}

	if uploadMeta.Key != key {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Track active upload
	metrics.UploadsActive.Inc()
	defer metrics.UploadsActive.Dec()

	// Handle AWS chunked encoding
	var body io.Reader = wrapBodyIfChunked(r.Body, r.Header.Get("Content-Encoding"), r.Header.Get("X-Amz-Content-Sha256"))

	// Enforce maximum part size limit
	if h.cfg.Limits.MaxPartSize > 0 {
		body = newLimitedReader(body, h.cfg.Limits.MaxPartSize)
	}

	partMeta, err := h.storage.UploadPart(uploadID, partNumber, body)
	if err != nil {
		if strings.Contains(err.Error(), "entity too large") {
			s3.WriteErrorResponse(w, s3.ErrEntityTooLarge)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")

	if uploadID == "" {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Verify upload exists and key matches
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrNoSuchUpload)
		return
	}

	if uploadMeta.Key != key {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Parse request body with size limit to prevent XML bomb attacks
	// Limit to 1MB which is more than enough for 10,000 parts
	const maxXMLBodySize = 1 * 1024 * 1024
	limitedBody := io.LimitReader(r.Body, maxXMLBodySize)
	var completeReq s3.CompleteMultipartUpload
	if err := xml.NewDecoder(limitedBody).Decode(&completeReq); err != nil {
		s3.WriteErrorResponse(w, s3.ErrMalformedXML)
		return
	}

	// Complete the upload
	objMeta, err := h.storage.CompleteMultipartUpload(uploadID, completeReq.Parts)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s3.WriteErrorResponse(w, s3.ErrInvalidPart)
			return
		}
		if strings.Contains(err.Error(), "order") {
			s3.WriteErrorResponse(w, s3.ErrInvalidPartOrder)
			return
		}
		s3.WriteErrorResponse(w, s3.ErrInternalError)
		return
	}

	result := s3.CompleteMultipartUploadResult{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket: bucket,
		Key:    key,
		ETag:   objMeta.ETag,
		// Location intentionally omitted to prevent host header injection
		// Clients should construct the URL from Bucket and Key if needed
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	query := r.URL.Query()
	uploadID := query.Get("uploadId")

	if uploadID == "" {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	// Verify upload exists
	uploadMeta, err := h.storage.GetMultipartUpload(uploadID)
	if err != nil {
		s3.WriteErrorResponse(w, s3.ErrNoSuchUpload)
		return
	}

	if uploadMeta.Key != key {
		s3.WriteErrorResponse(w, s3.ErrInvalidArgument)
		return
	}

	if err := h.storage.AbortMultipartUpload(uploadID); err != nil {
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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

	s3.WriteErrorResponse(w, s3.ErrInvalidRequest)
}

// GetBucket handles GET /{bucket} for listing objects or bucket operations
func (h *Handlers) GetBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")

	if bucket != h.cfg.Bucket.Name {
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
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

	maxKeys := maxKeysLimit
	if maxKeysStr := query.Get("max-keys"); maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
			// Cap max-keys immediately to prevent resource exhaustion
			if maxKeys > maxKeysLimit {
				maxKeys = maxKeysLimit
			}
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
		s3.WriteErrorResponse(w, s3.ErrInternalError)
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
		s3.WriteErrorResponse(w, s3.ErrNoSuchBucket)
		return
	}

	query := r.URL.Query()

	if query.Has("delete") {
		h.DeleteObjects(w, r)
		return
	}

	s3.WriteErrorResponse(w, s3.ErrInvalidRequest)
}

// DeleteObjects handles POST /{bucket}?delete (batch delete)
func (h *Handlers) DeleteObjects(w http.ResponseWriter, r *http.Request) {
	// Note: bucket validation is done in PostBucket before calling this handler
	_ = r.PathValue("bucket")

	// Parse request body with size limit to prevent XML bomb attacks
	// Limit to 1MB which is more than enough for batch delete requests
	const maxXMLBodySize = 1 * 1024 * 1024
	limitedBody := io.LimitReader(r.Body, maxXMLBodySize)
	var deleteReq s3.Delete
	if err := xml.NewDecoder(limitedBody).Decode(&deleteReq); err != nil {
		s3.WriteErrorResponse(w, s3.ErrMalformedXML)
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
