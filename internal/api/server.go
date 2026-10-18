package api

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// Server timeout constants
const (
	// ReadHeaderTimeout is the amount of time allowed to read request headers.
	// This helps mitigate Slowloris attacks.
	ReadHeaderTimeout = 10 * time.Second

	// IdleTimeout is the maximum amount of time to wait for the next request.
	IdleTimeout = 120 * time.Second

	// MaxHeaderBytes is the maximum size of request headers.
	MaxHeaderBytes = 1 << 20 // 1 MB
)

// Server is the S3 HTTP server
type Server struct {
	cfg        *config.Config
	handlers   *Handlers
	routes     map[string]routes
	httpServer *http.Server
}

// NewServer creates a new S3 server
func NewServer(cfg *config.Config, store storage.MultipartStorage) *Server {
	s := &Server{
		cfg:      cfg,
		handlers: NewHandlers(cfg, store),
	}
	s.setupRoutes()

	s.httpServer = &http.Server{
		Addr:              cfg.Server.Address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       cfg.Server.ReadTimeout,
		WriteTimeout:      cfg.Server.WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}

	return s
}

// routes holds the handlers of one method for bucket paths ("/{bucket}")
// and object paths ("/{bucket}/{key...}")
type routes struct {
	bucket, object http.Handler
}

// allowedMethods is the Allow header of a 405 response
const allowedMethods = "DELETE, GET, HEAD, POST, PUT"

// setupRoutes configures the HTTP routes
func (s *Server) setupRoutes() {
	authMiddleware := AuthMiddleware(s.cfg)
	read := func(h http.HandlerFunc) http.Handler {
		return MetricsMiddleware(authMiddleware(h))
	}
	write := func(h http.HandlerFunc) http.Handler {
		return MetricsMiddleware(authMiddleware(RequireWritePrivilege(h)))
	}

	s.routes = map[string]routes{
		http.MethodHead:   {bucket: read(s.handlers.HeadBucket), object: read(s.handlers.HeadObject)},
		http.MethodGet:    {bucket: read(s.handlers.GetBucket), object: read(s.handlers.GetObject)},
		http.MethodPut:    {bucket: write(s.handlers.CreateBucket), object: write(s.handlers.PutObject)},
		http.MethodDelete: {bucket: write(s.handlers.DeleteBucket), object: write(s.handlers.DeleteObject)},
		http.MethodPost:   {bucket: write(s.handlers.PostBucket), object: write(s.handlers.PostObject)},
	}
}

// route dispatches a request on its method and on whether its path is a
// bucket path ("/{bucket}") or an object path ("/{bucket}/{key...}"), and
// sets the bucket and key path values. It does not use http.ServeMux,
// which redirects paths with repeated slashes or "." segments to a cleaned
// path: in S3 those are part of the key, so "a//b" and "a/b" are different
// objects, and a PUT of "a//b" must not be redirected to "a/b".
func (s *Server) route(w http.ResponseWriter, r *http.Request) {
	bucket, key, isObject := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if bucket == "" || !strings.HasPrefix(r.URL.Path, "/") {
		http.NotFound(w, r)
		return
	}
	rt, ok := s.routes[r.Method]
	if !ok {
		w.Header().Set("Allow", allowedMethods)
		s3.WriteErrorResponse(w, s3.ErrMethodNotAllowed)
		return
	}
	r.SetPathValue("bucket", bucket)
	if isObject {
		r.SetPathValue("key", key)
		rt.object.ServeHTTP(w, r)
		return
	}
	rt.bucket.ServeHTTP(w, r)
}

// Handler returns the HTTP handler for the S3 listener, including health checks
func (s *Server) Handler() http.Handler {
	// Handle health checks before the S3 routes, where they would be
	// bucket names. /metrics is served only on the separate metrics
	// listener.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		case "/readyz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		case "/favicon.ico",
			"/robots.txt",
			"/apple-touch-icon.png",
			"/apple-touch-icon-precomposed.png":
			// Avoid authentication check
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.route(w, r)
	})
	// Apply middlewares: RequestID first, then AccessLog
	return RequestIDMiddleware(AccessLogMiddleware(s.cfg.Server.TrustedProxies)(handler))
}

// ListenAndServe starts the server with security-hardened timeouts
func (s *Server) ListenAndServe() error {
	slog.Info("starting S3 server", "address", s.cfg.Server.Address)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server without interrupting active connections
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down server gracefully")
	return s.httpServer.Shutdown(ctx)
}
