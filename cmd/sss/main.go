package main

import (
	"flag"
	"log"
	"os"

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

	// Create and start server
	server := api.NewServer(cfg, store)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server error: %v", err)
		os.Exit(1)
	}
}
