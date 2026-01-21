package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/espen/stupid-simple-s3/internal/api"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/storage"
	"github.com/espen/stupid-simple-s3/internal/version"
)

// ShutdownTimeout is the maximum time to wait for graceful shutdown
const ShutdownTimeout = 120 * time.Second

func main() {
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version.String())
		os.Exit(0)
	}

	// Load configuration from environment variables
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Configure structured logging
	configureLogger(cfg.Log.Format, cfg.Log.Level)

	slog.Info("starting stupid-simple-s3", "version", version.String())

	cfg.LogConfiguration()

	// Initialize storage (creates directories if they don't exist)
	store, err := storage.NewFilesystemStorage(cfg.Storage.Path, cfg.Storage.MultipartPath)
	if err != nil {
		slog.Error("failed to initialize storage", "error", err)
		os.Exit(1)
	}

	// Start cleanup job if enabled
	if cfg.Cleanup.Enabled {
		go runCleanupJob(store, cfg.Cleanup.GetInterval(), cfg.Cleanup.GetMaxAge())
	}

	// Create server
	server := api.NewServer(cfg, store)

	// Start server in goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	slog.Info("received shutdown signal", "signal", sig.String())

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("server stopped")
}

// configureLogger sets up the default slog logger
func configureLogger(format, level string) {
	opts := &slog.HandlerOptions{
		Level: parseLogLevel(level),
	}

	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	slog.SetDefault(slog.New(handler))
}

// parseLogLevel converts a string log level to slog.Level
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// runCleanupJob periodically cleans up stale multipart uploads
func runCleanupJob(store storage.MultipartStorage, interval, maxAge time.Duration) {
	slog.Info("starting multipart upload cleanup job",
		"interval", interval.String(),
		"max_age", maxAge.String(),
	)

	// Run immediately on startup
	cleaned, err := store.CleanupStaleUploads(maxAge)
	if err != nil {
		slog.Error("cleanup error", "error", err)
	} else if cleaned > 0 {
		slog.Info("cleaned up stale multipart uploads", "count", cleaned)
	}

	// Then run periodically
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		cleaned, err := store.CleanupStaleUploads(maxAge)
		if err != nil {
			slog.Error("cleanup error", "error", err)
		} else if cleaned > 0 {
			slog.Info("cleaned up stale multipart uploads", "count", cleaned)
		}
	}
}
