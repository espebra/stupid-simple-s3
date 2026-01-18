package api

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/espen/stupid-simple-s3/internal/auth"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/metrics"
	"github.com/espen/stupid-simple-s3/internal/s3"
)

type contextKey string

const (
	credentialContextKey contextKey = "credential"
	operationContextKey  contextKey = "operation"
)

// responseWriter wraps http.ResponseWriter to capture status code and bytes written
type responseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// MetricsMiddleware collects request metrics
func MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics.RequestsInFlight.Inc()
		defer metrics.RequestsInFlight.Dec()

		start := time.Now()
		rw := newResponseWriter(w)

		next.ServeHTTP(rw, r)

		duration := time.Since(start).Seconds()
		operation := getOperationFromContext(r)
		status := strconv.Itoa(rw.statusCode)

		metrics.RequestsTotal.WithLabelValues(r.Method, operation, status).Inc()
		metrics.RequestDuration.WithLabelValues(r.Method, operation).Observe(duration)

		if rw.bytesWritten > 0 {
			metrics.BytesSent.WithLabelValues(operation).Add(float64(rw.bytesWritten))
		}

		if r.ContentLength > 0 {
			metrics.BytesReceived.WithLabelValues(operation).Add(float64(r.ContentLength))
		}

		// Track errors
		if rw.statusCode >= 400 {
			errorCode := getErrorCodeFromStatus(rw.statusCode)
			metrics.ErrorsTotal.WithLabelValues(operation, errorCode).Inc()
		}
	})
}

// AccessLogMiddleware logs HTTP requests in a common log format
func AccessLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := newResponseWriter(w)

		next.ServeHTTP(rw, r)

		duration := time.Since(start)
		clientIP := getClientIP(r)

		log.Printf("%s %s %s %d %d %s",
			clientIP,
			r.Method,
			r.URL.RequestURI(),
			rw.statusCode,
			rw.bytesWritten,
			duration,
		)
	})
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (for proxies)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the list
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Check X-Real-IP header
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	// Fall back to RemoteAddr
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// SetOperation middleware sets the operation name in the request context
func SetOperation(operation string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), operationContextKey, operation)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func getOperationFromContext(r *http.Request) string {
	if op, ok := r.Context().Value(operationContextKey).(string); ok {
		return op
	}
	return detectOperation(r)
}

// detectOperation determines the S3 operation from the request
func detectOperation(r *http.Request) string {
	query := r.URL.Query()

	switch r.Method {
	case "HEAD":
		if strings.Count(r.URL.Path, "/") > 1 {
			return metrics.OpHeadObject
		}
		return metrics.OpHeadBucket

	case "GET":
		return metrics.OpGetObject

	case "PUT":
		if query.Has("uploadId") && query.Has("partNumber") {
			return metrics.OpUploadPart
		}
		return metrics.OpPutObject

	case "DELETE":
		if query.Has("uploadId") {
			return metrics.OpAbortMultipartUpload
		}
		return metrics.OpDeleteObject

	case "POST":
		if query.Has("uploads") {
			return metrics.OpCreateMultipartUpload
		}
		if query.Has("uploadId") {
			return metrics.OpCompleteMultipartUpload
		}
	}

	return metrics.OpUnknown
}

func getErrorCodeFromStatus(status int) string {
	switch status {
	case http.StatusNotFound:
		return "not_found"
	case http.StatusForbidden:
		return "forbidden"
	case http.StatusBadRequest:
		return "bad_request"
	case http.StatusInternalServerError:
		return "internal_error"
	default:
		return "other"
	}
}

// AuthMiddleware creates middleware that verifies AWS Signature v4 authentication
func AuthMiddleware(cfg *config.Config) func(http.Handler) http.Handler {
	sigv4 := &auth.SignatureV4{}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check if this is a presigned URL request
			if auth.IsPresignedRequest(r) {
				handlePresignedAuth(w, r, cfg, sigv4, next)
				return
			}

			// Parse authorization header to get access key ID
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMissingHeader).Inc()
				s3.NewError(s3.ErrAccessDenied, r.URL.Path).WriteResponse(w)
				return
			}

			parsed, err := sigv4.ParseAuthorization(authHeader)
			if err != nil {
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMalformedHeader).Inc()
				s3.NewError(s3.ErrAuthorizationHeaderMalformed, r.URL.Path).WriteResponse(w)
				return
			}

			// Look up credential
			cred := cfg.GetCredential(parsed.AccessKeyID)
			if cred == nil {
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonInvalidAccessKey).Inc()
				s3.NewError(s3.ErrInvalidAccessKeyId, r.URL.Path).WriteResponse(w)
				return
			}

			// Verify signature
			_, err = sigv4.VerifyRequest(r, cred.SecretAccessKey)
			if err != nil {
				// Check for specific error types
				if strings.Contains(err.Error(), "skewed") {
					metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
					s3.NewError(s3.ErrRequestTimeTooSkewed, r.URL.Path).WriteResponse(w)
					return
				}
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonSignatureMismatch).Inc()
				s3.NewError(s3.ErrSignatureDoesNotMatch, r.URL.Path).WriteResponse(w)
				return
			}

			// Store credential in context for handlers to check privileges
			ctx := context.WithValue(r.Context(), credentialContextKey, cred)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// handlePresignedAuth handles authentication for presigned URL requests
func handlePresignedAuth(w http.ResponseWriter, r *http.Request, cfg *config.Config, sigv4 *auth.SignatureV4, next http.Handler) {
	// Get access key ID from presigned URL
	accessKeyID := auth.GetPresignedAccessKeyID(r)
	if accessKeyID == "" {
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMalformedHeader).Inc()
		s3.NewError(s3.ErrAuthorizationHeaderMalformed, r.URL.Path).WriteResponse(w)
		return
	}

	// Look up credential
	cred := cfg.GetCredential(accessKeyID)
	if cred == nil {
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonInvalidAccessKey).Inc()
		s3.NewError(s3.ErrInvalidAccessKeyId, r.URL.Path).WriteResponse(w)
		return
	}

	// Verify presigned URL signature
	_, err := sigv4.VerifyPresignedRequest(r, cred.SecretAccessKey)
	if err != nil {
		// Check for specific error types
		if strings.Contains(err.Error(), "expired") {
			metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
			s3.NewError(s3.ErrExpiredToken, r.URL.Path).WriteResponse(w)
			return
		}
		if strings.Contains(err.Error(), "future") {
			metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
			s3.NewError(s3.ErrRequestTimeTooSkewed, r.URL.Path).WriteResponse(w)
			return
		}
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonSignatureMismatch).Inc()
		s3.NewError(s3.ErrSignatureDoesNotMatch, r.URL.Path).WriteResponse(w)
		return
	}

	// Store credential in context for handlers to check privileges
	ctx := context.WithValue(r.Context(), credentialContextKey, cred)
	next.ServeHTTP(w, r.WithContext(ctx))
}

// GetCredential retrieves the authenticated credential from the request context
func GetCredential(r *http.Request) *config.Credential {
	cred, ok := r.Context().Value(credentialContextKey).(*config.Credential)
	if !ok {
		return nil
	}
	return cred
}

// RequireWritePrivilege middleware checks if the credential has write privilege
func RequireWritePrivilege(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cred := GetCredential(r)
		if cred == nil || !cred.CanWrite() {
			metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonAccessDenied).Inc()
			s3.NewError(s3.ErrAccessDenied, r.URL.Path).WriteResponse(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// MetricsBasicAuth creates middleware that requires basic auth for the metrics endpoint.
// If username and password are both empty, anonymous access is allowed.
func MetricsBasicAuth(username, password string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Allow anonymous access if no credentials are configured
			if username == "" && password == "" {
				next.ServeHTTP(w, r)
				return
			}

			// Check basic auth credentials
			u, p, ok := r.BasicAuth()
			if !ok || u != username || p != password {
				w.Header().Set("WWW-Authenticate", `Basic realm="metrics"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
