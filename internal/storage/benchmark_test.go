package storage

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

func setupBenchStorage(b *testing.B) (*FilesystemStorage, func()) {
	b.Helper()

	tmpDir, err := os.MkdirTemp("", "sss-bench-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	basePath := filepath.Join(tmpDir, "data")
	multipartPath := filepath.Join(tmpDir, "multipart")

	storage, err := NewFilesystemStorage(basePath, multipartPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("failed to create storage: %v", err)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return storage, cleanup
}

func BenchmarkPutObject(b *testing.B) {
	sizes := []int{
		1 * 1024,         // 1KB
		64 * 1024,        // 64KB
		1024 * 1024,      // 1MB
		10 * 1024 * 1024, // 10MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			storage, cleanup := setupBenchStorage(b)
			defer cleanup()

			// Pre-generate random content
			content := make([]byte, size)
			_, _ = rand.Read(content)

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench-object-%d", i)
				_, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content))
				if err != nil {
					b.Fatalf("PutObject failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkGetObject(b *testing.B) {
	sizes := []int{
		1 * 1024,         // 1KB
		64 * 1024,        // 64KB
		1024 * 1024,      // 1MB
		10 * 1024 * 1024, // 10MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			storage, cleanup := setupBenchStorage(b)
			defer cleanup()

			// Create object first
			content := make([]byte, size)
			_, _ = rand.Read(content)
			key := "bench-get-object"
			if _, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content)); err != nil {
				b.Fatalf("PutObject failed: %v", err)
			}

			b.SetBytes(int64(size))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader, _, err := storage.GetObject(key)
				if err != nil {
					b.Fatalf("GetObject failed: %v", err)
				}
				_, _ = io.Copy(io.Discard, reader)
				_ = reader.Close()
			}
		})
	}
}

func BenchmarkHeadObject(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	// Create object first
	key := "bench-head-object"
	if _, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("content"))); err != nil {
		b.Fatalf("PutObject failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.HeadObject(key)
		if err != nil {
			b.Fatalf("HeadObject failed: %v", err)
		}
	}
}

func BenchmarkDeleteObject(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	// Pre-create objects
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-delete-%d", i)
		if _, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("content"))); err != nil {
			b.Fatalf("PutObject failed: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-delete-%d", i)
		err := storage.DeleteObject(key)
		if err != nil {
			b.Fatalf("DeleteObject failed: %v", err)
		}
	}
}

func BenchmarkObjectExists(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	key := "bench-exists-object"
	if _, err := storage.PutObject(key, "text/plain", nil, bytes.NewReader([]byte("content"))); err != nil {
		b.Fatalf("PutObject failed: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.ObjectExists(key)
		if err != nil {
			b.Fatalf("ObjectExists failed: %v", err)
		}
	}
}

func BenchmarkMultipartUpload(b *testing.B) {
	partSizes := []int{
		5 * 1024 * 1024,  // 5MB (minimum S3 part size)
		10 * 1024 * 1024, // 10MB
	}

	numParts := []int{2, 5, 10}

	for _, partSize := range partSizes {
		for _, parts := range numParts {
			name := fmt.Sprintf("partSize=%dMB_parts=%d", partSize/(1024*1024), parts)
			b.Run(name, func(b *testing.B) {
				storage, cleanup := setupBenchStorage(b)
				defer cleanup()

				// Pre-generate part content
				partContent := make([]byte, partSize)
				_, _ = rand.Read(partContent)

				totalSize := int64(partSize * parts)
				b.SetBytes(totalSize)
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					key := fmt.Sprintf("bench-multipart-%d", i)

					uploadID, err := storage.CreateMultipartUpload(key, "application/octet-stream", nil)
					if err != nil {
						b.Fatalf("CreateMultipartUpload failed: %v", err)
					}

					var completedParts []s3.CompletedPartInput
					for p := 1; p <= parts; p++ {
						partMeta, err := storage.UploadPart(uploadID, p, bytes.NewReader(partContent))
						if err != nil {
							b.Fatalf("UploadPart failed: %v", err)
						}
						completedParts = append(completedParts, s3.CompletedPartInput{
							PartNumber: p,
							ETag:       partMeta.ETag,
						})
					}

					_, err = storage.CompleteMultipartUpload(uploadID, completedParts)
					if err != nil {
						b.Fatalf("CompleteMultipartUpload failed: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkConcurrentPutObject(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	content := make([]byte, 64*1024) // 64KB
	_, _ = rand.Read(content)

	var counter atomic.Int64

	b.SetBytes(int64(len(content)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			key := fmt.Sprintf("bench-concurrent-%d", i)
			_, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content))
			if err != nil {
				b.Errorf("PutObject failed: %v", err)
			}
		}
	})
}

func BenchmarkConcurrentGetObject(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	// Create object
	content := make([]byte, 64*1024) // 64KB
	_, _ = rand.Read(content)
	key := "bench-concurrent-get"
	if _, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content)); err != nil {
		b.Fatalf("PutObject failed: %v", err)
	}

	b.SetBytes(int64(len(content)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reader, _, err := storage.GetObject(key)
			if err != nil {
				b.Errorf("GetObject failed: %v", err)
				continue
			}
			_, _ = io.Copy(io.Discard, reader)
			_ = reader.Close()
		}
	})
}

func BenchmarkMixedWorkload(b *testing.B) {
	storage, cleanup := setupBenchStorage(b)
	defer cleanup()

	content := make([]byte, 16*1024) // 16KB
	_, _ = rand.Read(content)

	// Pre-populate some objects
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("preload-%d", i)
		if _, err := storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content)); err != nil {
			b.Fatalf("PutObject failed: %v", err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			op := i % 10
			switch {
			case op < 3: // 30% writes
				key := fmt.Sprintf("mixed-write-%d", i)
				_, _ = storage.PutObject(key, "application/octet-stream", nil, bytes.NewReader(content))
			case op < 8: // 50% reads
				key := fmt.Sprintf("preload-%d", i%100)
				reader, _, err := storage.GetObject(key)
				if err == nil {
					_, _ = io.Copy(io.Discard, reader)
					_ = reader.Close()
				}
			default: // 20% head
				key := fmt.Sprintf("preload-%d", i%100)
				_, _ = storage.HeadObject(key)
			}
			i++
		}
	})
}
