package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RequestsInFlight tracks the number of requests currently being processed
	RequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "sss_http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
	)

	// RequestsTotal counts total HTTP requests by method, path pattern, and status code
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sss_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "operation", "status"},
	)

	// RequestDuration tracks request latency in seconds
	RequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sss_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		},
		[]string{"method", "operation"},
	)

	// BytesReceived counts bytes received in request bodies
	BytesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sss_http_request_bytes_total",
			Help: "Total bytes received in HTTP request bodies",
		},
		[]string{"operation"},
	)

	// BytesSent counts bytes sent in response bodies
	BytesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sss_http_response_bytes_total",
			Help: "Total bytes sent in HTTP response bodies",
		},
		[]string{"operation"},
	)

	// ErrorsTotal counts errors by type
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sss_errors_total",
			Help: "Total number of errors",
		},
		[]string{"operation", "error_code"},
	)

	// ObjectsStored tracks number of objects currently stored (gauge)
	ObjectsStored = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "sss_objects_stored",
			Help: "Number of objects currently stored",
		},
	)

	// StorageBytesUsed tracks total bytes used by stored objects (gauge)
	StorageBytesUsed = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "sss_storage_bytes_used",
			Help: "Total bytes used by stored objects",
		},
	)

	// MultipartUploadsActive tracks number of active multipart uploads
	MultipartUploadsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "sss_multipart_uploads_active",
			Help: "Number of active multipart uploads",
		},
	)

	// AuthFailuresTotal counts authentication failures
	AuthFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sss_auth_failures_total",
			Help: "Total number of authentication failures",
		},
		[]string{"reason"},
	)
)

// Operation names for consistent labeling
const (
	OpPutObject               = "PutObject"
	OpGetObject               = "GetObject"
	OpHeadObject              = "HeadObject"
	OpDeleteObject            = "DeleteObject"
	OpHeadBucket              = "HeadBucket"
	OpCreateMultipartUpload   = "CreateMultipartUpload"
	OpUploadPart              = "UploadPart"
	OpCompleteMultipartUpload = "CompleteMultipartUpload"
	OpAbortMultipartUpload    = "AbortMultipartUpload"
	OpListObjects             = "ListObjects"
	OpCopyObject              = "CopyObject"
	OpDeleteObjects           = "DeleteObjects"
	OpUnknown                 = "Unknown"
)

// Auth failure reasons
const (
	AuthReasonMissingHeader     = "missing_header"
	AuthReasonMalformedHeader   = "malformed_header"
	AuthReasonInvalidAccessKey  = "invalid_access_key"
	AuthReasonSignatureMismatch = "signature_mismatch"
	AuthReasonTimeSkew          = "time_skew"
	AuthReasonAccessDenied      = "access_denied"
)
