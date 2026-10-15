package api

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/espen/stupid-simple-s3/internal/config"
)

// NewMetricsServer creates the HTTP server for the separate metrics listener
// configured by cfg.Server.MetricsAddress. It serves /metrics and, when
// cfg.Pprof.Enabled is set, the pprof endpoints under /debug/pprof/. All
// endpoints share the metrics basic auth.
func NewMetricsServer(cfg *config.Config) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if cfg.Pprof.Enabled {
		registerPprof(mux)
	}

	metricsAuth := MetricsBasicAuth(cfg.MetricsAuth.Username, cfg.MetricsAuth.Password)
	handler := RequestIDMiddleware(accessLogMiddleware(cfg.Server.TrustedProxies, isMetricsEndpoint)(metricsAuth(mux)))

	return &http.Server{
		Addr:              cfg.Server.MetricsAddress,
		Handler:           handler,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       IdleTimeout,
		MaxHeaderBytes:    MaxHeaderBytes,
	}
}

// registerPprof registers the standard net/http/pprof endpoints under
// /debug/pprof/ on mux.
func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
