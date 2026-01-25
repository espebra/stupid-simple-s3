package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
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
	requestIDContextKey  contextKey = "request_id"
)

const requestIDHeader = "X-Request-ID"

// authFailureDelay is the duration to sleep on authentication failures
// This slows down brute-force attacks without complex rate limiting
const authFailureDelay = 100 * time.Millisecond

// trustedProxyChecker validates if a request comes from a trusted proxy
type trustedProxyChecker struct {
	cidrs []*net.IPNet
	ips   map[string]bool
}

// newTrustedProxyChecker creates a checker from a list of trusted proxy IPs/CIDRs
func newTrustedProxyChecker(trustedProxies []string) *trustedProxyChecker {
	checker := &trustedProxyChecker{
		ips: make(map[string]bool),
	}

	for _, proxy := range trustedProxies {
		proxy = strings.TrimSpace(proxy)
		if proxy == "" {
			continue
		}

		// Try to parse as CIDR
		if _, cidr, err := net.ParseCIDR(proxy); err == nil {
			checker.cidrs = append(checker.cidrs, cidr)
		} else if ip := net.ParseIP(proxy); ip != nil {
			// Plain IP
			checker.ips[ip.String()] = true
		}
	}

	return checker
}

// isTrusted checks if an IP is from a trusted proxy
func (c *trustedProxyChecker) isTrusted(ipStr string) bool {
	// If no trusted proxies configured, don't trust any proxy headers
	if len(c.cidrs) == 0 && len(c.ips) == 0 {
		return false
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	// Check exact IP match
	if c.ips[ip.String()] {
		return true
	}

	// Check CIDR ranges
	for _, cidr := range c.cidrs {
		if cidr.Contains(ip) {
			return true
		}
	}

	return false
}

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

// countingReader wraps io.ReadCloser to count bytes read
type countingReader struct {
	io.ReadCloser
	bytesRead int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.ReadCloser.Read(p)
	cr.bytesRead += int64(n)
	return n, err
}

// generateRequestID generates a random request ID
func generateRequestID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "unknown"
	}
	return hex.EncodeToString(b)
}

// RequestIDMiddleware adds a request ID to the context and response header
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(requestIDHeader)
		if requestID == "" {
			requestID = generateRequestID()
		}

		// Set request ID in response header
		w.Header().Set(requestIDHeader, requestID)

		// Add to context
		ctx := context.WithValue(r.Context(), requestIDContextKey, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// GetRequestID retrieves the request ID from the request context
func GetRequestID(r *http.Request) string {
	if id, ok := r.Context().Value(requestIDContextKey).(string); ok {
		return id
	}
	return ""
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

// AccessLogMiddleware logs HTTP requests using structured logging
func AccessLogMiddleware(trustedProxies []string) func(http.Handler) http.Handler {
	proxyChecker := newTrustedProxyChecker(trustedProxies)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := newResponseWriter(w)
			cr := &countingReader{ReadCloser: r.Body}
			r.Body = cr

			next.ServeHTTP(rw, r)

			duration := time.Since(start)
				clientIP := getClientIPWithTrust(r, proxyChecker)
			requestID := GetRequestID(r)

			// Log health and metrics endpoints at debug level to reduce noise
			if isInternalEndpoint(r.URL.Path) {
				slog.Debug("request",
					"client_ip", clientIP,
					"method", r.Method,
					"path", r.URL.Path,
					"status", rw.statusCode,
					"duration", duration.String(),
				)
				return
			}

			operation := getOperationFromContext(r)

			slog.Info("request",
				"client_ip", clientIP,
				"method", r.Method,
				"path", r.URL.RequestURI(),
				"status", rw.statusCode,
				"bytes_in", cr.bytesRead,
				"bytes_out", rw.bytesWritten,
				"duration", duration.String(),
				"request_id", requestID,
				"operation", operation,
			)
		})
	}
}

// isInternalEndpoint returns true for health check and metrics endpoints
func isInternalEndpoint(path string) bool {
	return path == "/healthz" || path == "/readyz" || path == "/metrics"
}

// getClientIP extracts the client IP from the request
// This version does NOT trust proxy headers - use getClientIPWithTrust for that
func getClientIP(r *http.Request) string {
	addr := r.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// getClientIPWithTrust extracts the client IP, trusting proxy headers only from trusted sources
func getClientIPWithTrust(r *http.Request, checker *trustedProxyChecker) string {
	remoteIP := getClientIP(r)

	// Only trust proxy headers if the direct connection is from a trusted proxy
	if checker != nil && checker.isTrusted(remoteIP) {
		// Check X-Forwarded-For header (for proxies)
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			// Take the first IP in the list (original client)
			if idx := strings.Index(xff, ","); idx != -1 {
				return strings.TrimSpace(xff[:idx])
			}
			return strings.TrimSpace(xff)
		}

		// Check X-Real-IP header
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return xri
		}
	}

	return remoteIP
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
				time.Sleep(authFailureDelay) // Slow down brute-force attempts
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMissingHeader).Inc()
				s3.WriteErrorResponse(w, s3.ErrAccessDenied)
				return
			}

			parsed, err := sigv4.ParseAuthorization(authHeader)
			if err != nil {
				time.Sleep(authFailureDelay)
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMalformedHeader).Inc()
				s3.WriteErrorResponse(w, s3.ErrAuthorizationHeaderMalformed)
				return
			}

			// Look up credential
			cred := cfg.GetCredential(parsed.AccessKeyID)
			if cred == nil {
				time.Sleep(authFailureDelay)
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonInvalidAccessKey).Inc()
				s3.WriteErrorResponse(w, s3.ErrInvalidAccessKeyId)
				return
			}

			// Verify signature
			_, err = sigv4.VerifyRequest(r, cred.SecretAccessKey)
			if err != nil {
				time.Sleep(authFailureDelay)
				// Check for specific error types
				if strings.Contains(err.Error(), "skewed") {
					metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
					s3.WriteErrorResponse(w, s3.ErrRequestTimeTooSkewed)
					return
				}
				metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonSignatureMismatch).Inc()
				s3.WriteErrorResponse(w, s3.ErrSignatureDoesNotMatch)
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
		time.Sleep(authFailureDelay)
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonMalformedHeader).Inc()
		s3.WriteErrorResponse(w, s3.ErrAuthorizationHeaderMalformed)
		return
	}

	// Look up credential
	cred := cfg.GetCredential(accessKeyID)
	if cred == nil {
		time.Sleep(authFailureDelay)
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonInvalidAccessKey).Inc()
		s3.WriteErrorResponse(w, s3.ErrInvalidAccessKeyId)
		return
	}

	// Verify presigned URL signature
	_, err := sigv4.VerifyPresignedRequest(r, cred.SecretAccessKey)
	if err != nil {
		time.Sleep(authFailureDelay)
		// Check for specific error types
		if strings.Contains(err.Error(), "expired") {
			metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
			s3.WriteErrorResponse(w, s3.ErrExpiredToken)
			return
		}
		if strings.Contains(err.Error(), "future") {
			metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonTimeSkew).Inc()
			s3.WriteErrorResponse(w, s3.ErrRequestTimeTooSkewed)
			return
		}
		metrics.AuthFailuresTotal.WithLabelValues(metrics.AuthReasonSignatureMismatch).Inc()
		s3.WriteErrorResponse(w, s3.ErrSignatureDoesNotMatch)
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
			s3.WriteErrorResponse(w, s3.ErrAccessDenied)
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
