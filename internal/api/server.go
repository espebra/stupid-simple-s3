package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/espen/stupid-simple-s3/internal/config"
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
	mux        *http.ServeMux
	httpServer *http.Server
}

// NewServer creates a new S3 server
func NewServer(cfg *config.Config, store storage.MultipartStorage) *Server {
	s := &Server{
		cfg:      cfg,
		handlers: NewHandlers(cfg, store),
		mux:      http.NewServeMux(),
	}
	s.setupRoutes()
	return s
}

// setupRoutes configures the HTTP routes
func (s *Server) setupRoutes() {
	authMiddleware := AuthMiddleware(s.cfg)

	// Bucket operations
	s.mux.Handle("HEAD /{bucket}", MetricsMiddleware(authMiddleware(http.HandlerFunc(s.handlers.HeadBucket))))
	s.mux.Handle("GET /{bucket}", MetricsMiddleware(authMiddleware(http.HandlerFunc(s.handlers.GetBucket))))
	s.mux.Handle("POST /{bucket}", MetricsMiddleware(authMiddleware(RequireWritePrivilege(http.HandlerFunc(s.handlers.PostBucket)))))

	// Object operations (read)
	s.mux.Handle("GET /{bucket}/{key...}", MetricsMiddleware(authMiddleware(http.HandlerFunc(s.handlers.GetObject))))
	s.mux.Handle("HEAD /{bucket}/{key...}", MetricsMiddleware(authMiddleware(http.HandlerFunc(s.handlers.HeadObject))))

	// Object operations (write) - require write privilege
	s.mux.Handle("PUT /{bucket}/{key...}", MetricsMiddleware(authMiddleware(RequireWritePrivilege(http.HandlerFunc(s.handlers.PutObject)))))
	s.mux.Handle("DELETE /{bucket}/{key...}", MetricsMiddleware(authMiddleware(RequireWritePrivilege(http.HandlerFunc(s.handlers.DeleteObject)))))
	s.mux.Handle("POST /{bucket}/{key...}", MetricsMiddleware(authMiddleware(RequireWritePrivilege(http.HandlerFunc(s.handlers.PostObject)))))
}

// Handler returns the HTTP handler that includes metrics endpoint
func (s *Server) Handler() http.Handler {
	// Wrap mux to handle /metrics before the S3 routes
	// This avoids Go 1.24+ routing conflicts between /metrics and /{bucket}
	metricsAuth := MetricsBasicAuth(s.cfg.MetricsAuth.Username, s.cfg.MetricsAuth.Password)
	metricsHandler := metricsAuth(promhttp.Handler())

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			metricsHandler.ServeHTTP(w, r)
			return
		case "/healthz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		case "/readyz":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		s.mux.ServeHTTP(w, r)
	})
	// Apply middlewares: RequestID first, then AccessLog
	return RequestIDMiddleware(AccessLogMiddleware(handler))
}

// ListenAndServe starts the server with security-hardened timeouts
func (s *Server) ListenAndServe() error {
	slog.Info("starting S3 server", "address", s.cfg.Server.Address)

	s.httpServer = &http.Server{
		Addr:              s.cfg.Server.Address,
		Handler:           s.Handler(),
		ReadHeaderTimeout: ReadHeaderTimeout,
		ReadTimeout:       s.cfg.Server.ReadTimeout,
		WriteTimeout:      s.cfg.Server.WriteTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}

	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server without interrupting active connections
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down server gracefully")
	return s.httpServer.Shutdown(ctx)
}
