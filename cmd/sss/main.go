package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/espen/stupid-simple-s3/internal/api"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize storage
	store, err := storage.NewFilesystemStorage(cfg.Storage.Path, cfg.Storage.MultipartPath)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}

	// Start cleanup job if enabled
	if cfg.Cleanup.Enabled {
		go runCleanupJob(store, cfg.Cleanup.GetInterval(), cfg.Cleanup.GetMaxAge())
	}

	// Create and start server
	server := api.NewServer(cfg, store)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
		os.Exit(1)
	}
}

// runCleanupJob periodically cleans up stale multipart uploads
func runCleanupJob(store storage.MultipartStorage, interval, maxAge time.Duration) {
	log.Printf("Starting multipart upload cleanup job (interval: %s, max age: %s)", interval, maxAge)

	// Run immediately on startup
	cleaned, err := store.CleanupStaleUploads(maxAge)
	if err != nil {
		log.Printf("Cleanup error: %v", err)
	} else if cleaned > 0 {
		log.Printf("Cleaned up %d stale multipart upload(s)", cleaned)
	}

	// Then run periodically
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		cleaned, err := store.CleanupStaleUploads(maxAge)
		if err != nil {
			log.Printf("Cleanup error: %v", err)
		} else if cleaned > 0 {
			log.Printf("Cleaned up %d stale multipart upload(s)", cleaned)
		}
	}
}
