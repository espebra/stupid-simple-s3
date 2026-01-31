// migrate-sha256 migrates object directories from the old base64-encoded
// naming scheme to SHA-256 hex digest names. Run this offline while
// stupid-simple-s3 is stopped. Use -dry-run to preview changes.
//
// Old scheme (MD5 + base64):
//   - Shard prefix: first 2 bytes of MD5 hash as hex (4 chars).
//   - Directory name: base64 URL encoding of the raw key. Length grows
//     proportionally with key length and can exceed the 255-byte filesystem
//     limit for keys longer than 189 bytes.
//
// New scheme (SHA-256):
//   - Shard prefix: first 2 bytes of SHA-256 hash as hex (4 chars).
//   - Directory name: full SHA-256 hex digest, always exactly 64 characters
//     regardless of key length.
//
// The original S3 key is recovered from meta.json, not from the directory
// name, so switching to a hash is safe.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

type objectMeta struct {
	Key string `json:"key"`
}

func main() {
	dataPath := flag.String("data", "", "path to the data directory (required)")
	dryRun := flag.Bool("dry-run", false, "preview changes without modifying anything")
	flag.Parse()

	if *dataPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: migrate-sha256 -data /path/to/data [-dry-run]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	bucketsPath := filepath.Join(*dataPath, "buckets")
	if _, err := os.Stat(bucketsPath); os.IsNotExist(err) {
		log.Fatalf("Buckets directory not found: %s", bucketsPath)
	}

	var migrated, skipped, errors int

	err := filepath.WalkDir(bucketsPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			log.Printf("Error accessing %s: %v", path, err)
			errors++
			return nil
		}

		if d.IsDir() || d.Name() != "meta.json" {
			return nil
		}

		// path is <buckets>/<bucket>/objects/<prefix>/<encodedKey>/meta.json
		objDir := filepath.Dir(path)
		prefixDir := filepath.Dir(objDir)
		objectsDir := filepath.Dir(prefixDir)

		// Verify we're inside an objects directory
		if filepath.Base(objectsDir) != "objects" {
			return nil
		}

		// Read meta.json to get the original key
		f, err := os.Open(path)
		if err != nil {
			log.Printf("Error opening %s: %v", path, err)
			errors++
			return nil
		}
		defer f.Close()

		var meta objectMeta
		if err := json.NewDecoder(f).Decode(&meta); err != nil {
			log.Printf("Error parsing %s: %v", path, err)
			errors++
			return nil
		}

		if meta.Key == "" {
			log.Printf("Empty key in %s, skipping", path)
			errors++
			return nil
		}

		// Compute new path
		keyHash := sha256.Sum256([]byte(meta.Key))
		newPrefix := hex.EncodeToString(keyHash[:2])
		newEncodedKey := hex.EncodeToString(keyHash[:])
		newPrefixDir := filepath.Join(objectsDir, newPrefix)
		newObjDir := filepath.Join(newPrefixDir, newEncodedKey)

		if objDir == newObjDir {
			skipped++
			return nil
		}

		if *dryRun {
			fmt.Printf("[dry-run] %s -> %s\n", objDir, newObjDir)
			migrated++
			return nil
		}

		// Create new prefix directory if needed
		if err := os.MkdirAll(newPrefixDir, 0700); err != nil {
			log.Printf("Error creating directory %s: %v", newPrefixDir, err)
			errors++
			return nil
		}

		// Rename the object directory
		if err := os.Rename(objDir, newObjDir); err != nil {
			log.Printf("Error renaming %s -> %s: %v", objDir, newObjDir, err)
			errors++
			return nil
		}

		fmt.Printf("Migrated: %s -> %s\n", objDir, newObjDir)
		migrated++

		// Try to remove old prefix directory if empty
		_ = os.Remove(prefixDir)

		return nil
	})

	if err != nil {
		log.Fatalf("Walk error: %v", err)
	}

	// Clean up empty prefix directories
	if !*dryRun {
		_ = filepath.WalkDir(bucketsPath, func(path string, d os.DirEntry, err error) error {
			if err != nil || !d.IsDir() {
				return nil
			}
			// Try to remove — only succeeds if empty
			_ = os.Remove(path)
			return nil
		})
	}

	fmt.Printf("\nMigration summary:\n")
	fmt.Printf("  Migrated: %d\n", migrated)
	fmt.Printf("  Skipped (already correct): %d\n", skipped)
	fmt.Printf("  Errors: %d\n", errors)
}
