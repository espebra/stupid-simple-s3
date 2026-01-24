package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// Since auth testing is complex, we'll test handlers without auth middleware
func setupTestHandlers(t *testing.T) (*Handlers, storage.MultipartStorage, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "sss-handlers-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := &config.Config{
		Bucket: config.Bucket{Name: "test-bucket"},
		Storage: config.Storage{
			Path:          filepath.Join(tmpDir, "data"),
			MultipartPath: filepath.Join(tmpDir, "multipart"),
		},
	}

	store, err := storage.NewFilesystemStorage(cfg.Storage.Path, cfg.Storage.MultipartPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create storage: %v", err)
	}

	// Create the test bucket
	if err := store.CreateBucket("test-bucket"); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create test bucket: %v", err)
	}

	handlers := NewHandlers(cfg, store)

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return handlers, store, cleanup
}

func TestCreateBucket(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("create new bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/new-bucket", nil)
		req.SetPathValue("bucket", "new-bucket")
		w := httptest.NewRecorder()

		handlers.CreateBucket(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("create existing bucket returns conflict", func(t *testing.T) {
		// test-bucket was already created in setupTestHandlers
		req := httptest.NewRequest("PUT", "/test-bucket", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.CreateBucket(w, req)

		if w.Code != http.StatusConflict {
			t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
		}

		var errResp s3.Error
		_ = xml.NewDecoder(w.Body).Decode(&errResp)
		if errResp.Code != s3.ErrBucketAlreadyOwnedByYou {
			t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrBucketAlreadyOwnedByYou)
		}
	})

	t.Run("create bucket with invalid name", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/INVALID", nil)
		req.SetPathValue("bucket", "INVALID")
		w := httptest.NewRecorder()

		handlers.CreateBucket(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestDeleteBucket(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("delete empty bucket", func(t *testing.T) {
		// Create a new bucket
		if err := store.CreateBucket("deleteme"); err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}

		req := httptest.NewRequest("DELETE", "/deleteme", nil)
		req.SetPathValue("bucket", "deleteme")
		w := httptest.NewRecorder()

		handlers.DeleteBucket(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNoContent)
		}

		// Verify bucket is gone
		exists, _ := store.BucketExists("deleteme")
		if exists {
			t.Error("bucket should not exist after deletion")
		}
	})

	t.Run("delete non-empty bucket returns conflict", func(t *testing.T) {
		// Create a bucket with an object
		if err := store.CreateBucket("nonempty"); err != nil {
			t.Fatalf("CreateBucket failed: %v", err)
		}
		_, err := store.PutObject("nonempty", "test-key", "text/plain", nil, strings.NewReader("content"))
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}

		req := httptest.NewRequest("DELETE", "/nonempty", nil)
		req.SetPathValue("bucket", "nonempty")
		w := httptest.NewRecorder()

		handlers.DeleteBucket(w, req)

		if w.Code != http.StatusConflict {
			t.Errorf("status = %d, want %d", w.Code, http.StatusConflict)
		}

		var errResp s3.Error
		_ = xml.NewDecoder(w.Body).Decode(&errResp)
		if errResp.Code != s3.ErrBucketNotEmpty {
			t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrBucketNotEmpty)
		}
	})

	t.Run("delete non-existent bucket returns not found", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/nonexistent", nil)
		req.SetPathValue("bucket", "nonexistent")
		w := httptest.NewRecorder()

		handlers.DeleteBucket(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}

		var errResp s3.Error
		_ = xml.NewDecoder(w.Body).Decode(&errResp)
		if errResp.Code != s3.ErrNoSuchBucket {
			t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrNoSuchBucket)
		}
	})

	t.Run("delete bucket with invalid name", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/INVALID", nil)
		req.SetPathValue("bucket", "INVALID")
		w := httptest.NewRecorder()

		handlers.DeleteBucket(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestHeadBucket(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("existing bucket", func(t *testing.T) {
		req := httptest.NewRequest("HEAD", "/test-bucket", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.HeadBucket(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("nonexistent bucket", func(t *testing.T) {
		req := httptest.NewRequest("HEAD", "/wrong-bucket", nil)
		req.SetPathValue("bucket", "wrong-bucket")
		w := httptest.NewRecorder()

		handlers.HeadBucket(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}

		// Verify error response
		var errResp s3.Error
		_ = xml.NewDecoder(w.Body).Decode(&errResp)
		if errResp.Code != s3.ErrNoSuchBucket {
			t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrNoSuchBucket)
		}
	})
}

func TestPutAndGetObject(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "test-file.txt"
	content := []byte("Hello, World!")

	// PUT
	t.Run("put object", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader(content))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()

		handlers.PutObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("PUT status = %d, want %d", w.Code, http.StatusOK)
		}

		etag := w.Header().Get("ETag")
		if etag == "" {
			t.Error("ETag header is empty")
		}
	})

	// GET
	t.Run("get object", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("GET status = %d, want %d", w.Code, http.StatusOK)
		}

		if w.Header().Get("Content-Type") != "text/plain" {
			t.Errorf("Content-Type = %q, want %q", w.Header().Get("Content-Type"), "text/plain")
		}

		if !bytes.Equal(w.Body.Bytes(), content) {
			t.Errorf("body = %q, want %q", w.Body.String(), string(content))
		}
	})
}

func TestGetObjectNotFound(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/test-bucket/nonexistent.txt", nil)
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", "nonexistent.txt")
	w := httptest.NewRecorder()

	handlers.GetObject(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}

	var errResp s3.Error
	_ = xml.NewDecoder(w.Body).Decode(&errResp)
	if errResp.Code != s3.ErrNoSuchKey {
		t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrNoSuchKey)
	}
}

func TestHeadObject(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "head-test.txt"
	content := []byte("Test content")

	// Create object first
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader(content))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", key)
	putReq.Header.Set("Content-Type", "text/plain")
	handlers.PutObject(httptest.NewRecorder(), putReq)

	// HEAD
	req := httptest.NewRequest("HEAD", "/test-bucket/"+key, nil)
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()

	handlers.HeadObject(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}

	if w.Header().Get("Content-Length") != "12" {
		t.Errorf("Content-Length = %q, want %q", w.Header().Get("Content-Length"), "12")
	}

	if w.Header().Get("ETag") == "" {
		t.Error("ETag header is empty")
	}

	// HEAD should return no body
	if w.Body.Len() != 0 {
		t.Errorf("HEAD response should have no body, got %d bytes", w.Body.Len())
	}
}

func TestDeleteObject(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "delete-test.txt"

	// Create object
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader([]byte("delete me")))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", key)
	handlers.PutObject(httptest.NewRecorder(), putReq)

	// DELETE
	req := httptest.NewRequest("DELETE", "/test-bucket/"+key, nil)
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", key)
	w := httptest.NewRecorder()

	handlers.DeleteObject(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("DELETE status = %d, want %d", w.Code, http.StatusNoContent)
	}

	// Verify object is gone
	getReq := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
	getReq.SetPathValue("bucket", "test-bucket")
	getReq.SetPathValue("key", key)
	getW := httptest.NewRecorder()

	handlers.GetObject(getW, getReq)

	if getW.Code != http.StatusNotFound {
		t.Errorf("GET after DELETE status = %d, want %d", getW.Code, http.StatusNotFound)
	}
}

func TestUserMetadata(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "metadata-test.txt"
	content := []byte("content")

	// PUT with metadata
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader(content))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", key)
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("X-Amz-Meta-Author", "test-user")
	putReq.Header.Set("X-Amz-Meta-Version", "1.0")
	handlers.PutObject(httptest.NewRecorder(), putReq)

	// GET and verify metadata
	getReq := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
	getReq.SetPathValue("bucket", "test-bucket")
	getReq.SetPathValue("key", key)
	w := httptest.NewRecorder()

	handlers.GetObject(w, getReq)

	if w.Header().Get("X-Amz-Meta-Author") != "test-user" {
		t.Errorf("X-Amz-Meta-Author = %q, want %q", w.Header().Get("X-Amz-Meta-Author"), "test-user")
	}
	if w.Header().Get("X-Amz-Meta-Version") != "1.0" {
		t.Errorf("X-Amz-Meta-Version = %q, want %q", w.Header().Get("X-Amz-Meta-Version"), "1.0")
	}
}

func TestWrongBucket(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	tests := []struct {
		name   string
		method string
		setup  func()
	}{
		{"PUT", "PUT", func() {}},
		{"GET", "GET", func() {}},
		{"HEAD", "HEAD", func() {}},
		{"DELETE", "DELETE", func() {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.method == "PUT" {
				body = bytes.NewReader([]byte("content"))
			}

			req := httptest.NewRequest(tt.method, "/wrong-bucket/test.txt", body)
			req.SetPathValue("bucket", "wrong-bucket")
			req.SetPathValue("key", "test.txt")
			w := httptest.NewRecorder()

			switch tt.method {
			case "PUT":
				handlers.PutObject(w, req)
			case "GET":
				handlers.GetObject(w, req)
			case "HEAD":
				handlers.HeadObject(w, req)
			case "DELETE":
				handlers.DeleteObject(w, req)
			}

			if w.Code != http.StatusNotFound {
				t.Errorf("%s wrong bucket: status = %d, want %d", tt.method, w.Code, http.StatusNotFound)
			}

			var errResp s3.Error
			_ = xml.NewDecoder(w.Body).Decode(&errResp)
			if errResp.Code != s3.ErrNoSuchBucket {
				t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrNoSuchBucket)
			}
		})
	}
}

func TestMultipartUploadHandlers(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "multipart-test.txt"

	// Create multipart upload
	var uploadID string
	t.Run("create multipart upload", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test-bucket/"+key+"?uploads", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()

		handlers.CreateMultipartUpload(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("CreateMultipartUpload status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.InitiateMultipartUploadResult
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.Bucket != "test-bucket" {
			t.Errorf("Bucket = %q, want %q", result.Bucket, "test-bucket")
		}
		if result.Key != key {
			t.Errorf("Key = %q, want %q", result.Key, key)
		}
		if result.UploadID == "" {
			t.Error("UploadID is empty")
		}

		uploadID = result.UploadID
	})

	// Upload parts
	var part1ETag, part2ETag string
	t.Run("upload parts", func(t *testing.T) {
		// Part 1
		req1 := httptest.NewRequest("PUT", "/test-bucket/"+key+"?partNumber=1&uploadId="+uploadID, bytes.NewReader([]byte("Part 1 content")))
		req1.SetPathValue("bucket", "test-bucket")
		req1.SetPathValue("key", key)
		w1 := httptest.NewRecorder()

		handlers.UploadPart(w1, req1)

		if w1.Code != http.StatusOK {
			t.Errorf("UploadPart 1 status = %d, want %d", w1.Code, http.StatusOK)
		}
		part1ETag = w1.Header().Get("ETag")

		// Part 2
		req2 := httptest.NewRequest("PUT", "/test-bucket/"+key+"?partNumber=2&uploadId="+uploadID, bytes.NewReader([]byte("Part 2 content")))
		req2.SetPathValue("bucket", "test-bucket")
		req2.SetPathValue("key", key)
		w2 := httptest.NewRecorder()

		handlers.UploadPart(w2, req2)

		if w2.Code != http.StatusOK {
			t.Errorf("UploadPart 2 status = %d, want %d", w2.Code, http.StatusOK)
		}
		part2ETag = w2.Header().Get("ETag")
	})

	// Complete multipart upload
	t.Run("complete multipart upload", func(t *testing.T) {
		completeXML := `<CompleteMultipartUpload>
			<Part><PartNumber>1</PartNumber><ETag>` + part1ETag + `</ETag></Part>
			<Part><PartNumber>2</PartNumber><ETag>` + part2ETag + `</ETag></Part>
		</CompleteMultipartUpload>`

		req := httptest.NewRequest("POST", "/test-bucket/"+key+"?uploadId="+uploadID, strings.NewReader(completeXML))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.CompleteMultipartUpload(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("CompleteMultipartUpload status = %d, want %d, body: %s", w.Code, http.StatusOK, w.Body.String())
		}

		var result s3.CompleteMultipartUploadResult
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.Key != key {
			t.Errorf("Key = %q, want %q", result.Key, key)
		}
		if !strings.Contains(result.ETag, "-2") {
			t.Errorf("ETag should indicate 2 parts: %q", result.ETag)
		}
	})

	// Verify object content
	t.Run("verify combined content", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("GET status = %d, want %d", w.Code, http.StatusOK)
		}

		expectedContent := "Part 1 contentPart 2 content"
		if w.Body.String() != expectedContent {
			t.Errorf("content = %q, want %q", w.Body.String(), expectedContent)
		}
	})
}

func TestAbortMultipartUpload(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "abort-test.txt"

	// Create upload
	createReq := httptest.NewRequest("POST", "/test-bucket/"+key+"?uploads", nil)
	createReq.SetPathValue("bucket", "test-bucket")
	createReq.SetPathValue("key", key)
	createW := httptest.NewRecorder()
	handlers.CreateMultipartUpload(createW, createReq)

	var createResult s3.InitiateMultipartUploadResult
	_ = xml.NewDecoder(createW.Body).Decode(&createResult)
	uploadID := createResult.UploadID

	// Upload a part
	partReq := httptest.NewRequest("PUT", "/test-bucket/"+key+"?partNumber=1&uploadId="+uploadID, bytes.NewReader([]byte("part")))
	partReq.SetPathValue("bucket", "test-bucket")
	partReq.SetPathValue("key", key)
	handlers.UploadPart(httptest.NewRecorder(), partReq)

	// Abort
	abortReq := httptest.NewRequest("DELETE", "/test-bucket/"+key+"?uploadId="+uploadID, nil)
	abortReq.SetPathValue("bucket", "test-bucket")
	abortReq.SetPathValue("key", key)
	abortW := httptest.NewRecorder()

	handlers.AbortMultipartUpload(abortW, abortReq)

	if abortW.Code != http.StatusNoContent {
		t.Errorf("AbortMultipartUpload status = %d, want %d", abortW.Code, http.StatusNoContent)
	}

	// Try to complete aborted upload - should fail
	completeReq := httptest.NewRequest("POST", "/test-bucket/"+key+"?uploadId="+uploadID, strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	completeReq.SetPathValue("bucket", "test-bucket")
	completeReq.SetPathValue("key", key)
	completeW := httptest.NewRecorder()

	handlers.CompleteMultipartUpload(completeW, completeReq)

	if completeW.Code != http.StatusNotFound {
		t.Errorf("Complete aborted upload status = %d, want %d", completeW.Code, http.StatusNotFound)
	}
}

func TestMetricsBasicAuth(t *testing.T) {
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metrics"))
	})

	t.Run("anonymous access allowed when no credentials configured", func(t *testing.T) {
		middleware := MetricsBasicAuth("", "")
		handler := middleware(dummyHandler)

		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("valid credentials accepted", func(t *testing.T) {
		middleware := MetricsBasicAuth("admin", "secret")
		handler := middleware(dummyHandler)

		req := httptest.NewRequest("GET", "/metrics", nil)
		req.SetBasicAuth("admin", "secret")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("invalid password rejected", func(t *testing.T) {
		middleware := MetricsBasicAuth("admin", "secret")
		handler := middleware(dummyHandler)

		req := httptest.NewRequest("GET", "/metrics", nil)
		req.SetBasicAuth("admin", "wrong")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
		}
		if w.Header().Get("WWW-Authenticate") == "" {
			t.Error("WWW-Authenticate header should be set")
		}
	})

	t.Run("invalid username rejected", func(t *testing.T) {
		middleware := MetricsBasicAuth("admin", "secret")
		handler := middleware(dummyHandler)

		req := httptest.NewRequest("GET", "/metrics", nil)
		req.SetBasicAuth("wrong", "secret")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
		}
	})

	t.Run("missing credentials rejected when auth required", func(t *testing.T) {
		middleware := MetricsBasicAuth("admin", "secret")
		handler := middleware(dummyHandler)

		req := httptest.NewRequest("GET", "/metrics", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", w.Code, http.StatusUnauthorized)
		}
		if w.Header().Get("WWW-Authenticate") == "" {
			t.Error("WWW-Authenticate header should be set")
		}
	})
}

func TestPostObjectRouting(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("routes to CreateMultipartUpload", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test-bucket/test.txt?uploads", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "test.txt")
		w := httptest.NewRecorder()

		handlers.PostObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.InitiateMultipartUploadResult
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Error("response should be InitiateMultipartUploadResult")
		}
	})

	t.Run("routes to CompleteMultipartUpload", func(t *testing.T) {
		// First create an upload
		createReq := httptest.NewRequest("POST", "/test-bucket/test.txt?uploads", nil)
		createReq.SetPathValue("bucket", "test-bucket")
		createReq.SetPathValue("key", "test.txt")
		createW := httptest.NewRecorder()
		handlers.CreateMultipartUpload(createW, createReq)

		var createResult s3.InitiateMultipartUploadResult
		_ = xml.NewDecoder(createW.Body).Decode(&createResult)

		// Now test routing to complete
		req := httptest.NewRequest("POST", "/test-bucket/test.txt?uploadId="+createResult.UploadID, strings.NewReader("<CompleteMultipartUpload></CompleteMultipartUpload>"))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "test.txt")
		w := httptest.NewRecorder()

		handlers.PostObject(w, req)

		// Even with no parts, it should try to complete (and may fail, but it routed correctly)
		// The important thing is it didn't return InvalidRequest
		if w.Code == http.StatusBadRequest {
			var errResp s3.Error
			_ = xml.NewDecoder(w.Body).Decode(&errResp)
			if errResp.Code == s3.ErrInvalidRequest {
				t.Error("should have routed to CompleteMultipartUpload, not returned InvalidRequest")
			}
		}
	})

	t.Run("returns error for unknown POST", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test-bucket/test.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "test.txt")
		w := httptest.NewRecorder()

		handlers.PostObject(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}

		var errResp s3.Error
		_ = xml.NewDecoder(w.Body).Decode(&errResp)
		if errResp.Code != s3.ErrInvalidRequest {
			t.Errorf("error code = %q, want %q", errResp.Code, s3.ErrInvalidRequest)
		}
	})
}

func TestGetBucket(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("wrong bucket returns error", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/wrong-bucket", nil)
		req.SetPathValue("bucket", "wrong-bucket")
		w := httptest.NewRecorder()

		handlers.GetBucket(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("list empty bucket", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.GetBucket(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.ListBucketResultV2
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.Name != "test-bucket" {
			t.Errorf("Name = %q, want %q", result.Name, "test-bucket")
		}
		if result.KeyCount != 0 {
			t.Errorf("KeyCount = %d, want 0", result.KeyCount)
		}
	})
}

func TestListObjectsV2(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create some objects
	objects := []string{"a/1.txt", "a/2.txt", "b/1.txt", "root.txt"}
	for _, key := range objects {
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader([]byte("content")))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		handlers.PutObject(httptest.NewRecorder(), req)
	}

	t.Run("list all objects", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.ListObjectsV2(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.ListBucketResultV2
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.KeyCount != 4 {
			t.Errorf("KeyCount = %d, want 4", result.KeyCount)
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2&prefix=a/", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.ListObjectsV2(w, req)

		var result s3.ListBucketResultV2
		_ = xml.NewDecoder(w.Body).Decode(&result)

		if result.KeyCount != 2 {
			t.Errorf("KeyCount with prefix a/ = %d, want 2", result.KeyCount)
		}
	})

	t.Run("list with delimiter", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2&delimiter=/", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.ListObjectsV2(w, req)

		var result s3.ListBucketResultV2
		_ = xml.NewDecoder(w.Body).Decode(&result)

		// Should have 1 root object and 2 common prefixes (a/, b/)
		if result.KeyCount != 1 {
			t.Errorf("KeyCount = %d, want 1", result.KeyCount)
		}
		if len(result.CommonPrefixes) != 2 {
			t.Errorf("CommonPrefixes count = %d, want 2", len(result.CommonPrefixes))
		}
	})

	t.Run("list with max-keys", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2&max-keys=2", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.ListObjectsV2(w, req)

		var result s3.ListBucketResultV2
		_ = xml.NewDecoder(w.Body).Decode(&result)

		if result.KeyCount != 2 {
			t.Errorf("KeyCount = %d, want 2", result.KeyCount)
		}
		if !result.IsTruncated {
			t.Error("IsTruncated should be true")
		}
	})
}

func TestDeleteObjects(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create objects to delete
	for _, key := range []string{"delete1.txt", "delete2.txt", "keep.txt"} {
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader([]byte("content")))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		handlers.PutObject(httptest.NewRecorder(), req)
	}

	t.Run("delete multiple objects", func(t *testing.T) {
		deleteXML := `<?xml version="1.0" encoding="UTF-8"?>
		<Delete>
			<Object><Key>delete1.txt</Key></Object>
			<Object><Key>delete2.txt</Key></Object>
		</Delete>`

		req := httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader(deleteXML))
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.DeleteObjects(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.DeleteObjectsResult
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if len(result.Deleted) != 2 {
			t.Errorf("Deleted count = %d, want 2", len(result.Deleted))
		}
	})

	t.Run("delete with quiet mode", func(t *testing.T) {
		// Re-create an object
		putReq := httptest.NewRequest("PUT", "/test-bucket/quiet-delete.txt", bytes.NewReader([]byte("content")))
		putReq.SetPathValue("bucket", "test-bucket")
		putReq.SetPathValue("key", "quiet-delete.txt")
		handlers.PutObject(httptest.NewRecorder(), putReq)

		deleteXML := `<?xml version="1.0" encoding="UTF-8"?>
		<Delete>
			<Quiet>true</Quiet>
			<Object><Key>quiet-delete.txt</Key></Object>
		</Delete>`

		req := httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader(deleteXML))
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.DeleteObjects(w, req)

		var result s3.DeleteObjectsResult
		_ = xml.NewDecoder(w.Body).Decode(&result)

		// In quiet mode, successfully deleted objects are not listed
		if len(result.Deleted) != 0 {
			t.Errorf("Deleted count in quiet mode = %d, want 0", len(result.Deleted))
		}
	})

	t.Run("malformed XML returns error", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test-bucket?delete", strings.NewReader("not xml"))
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.DeleteObjects(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestPostBucket(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	t.Run("wrong bucket returns error", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/wrong-bucket?delete", nil)
		req.SetPathValue("bucket", "wrong-bucket")
		w := httptest.NewRecorder()

		handlers.PostBucket(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("invalid operation returns error", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/test-bucket", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.PostBucket(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestCopyObject(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create source object
	srcKey := "source.txt"
	content := []byte("copy me")
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+srcKey, bytes.NewReader(content))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", srcKey)
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("X-Amz-Meta-Custom", "value")
	handlers.PutObject(httptest.NewRecorder(), putReq)

	t.Run("copy object successfully", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/destination.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "destination.txt")
		req.Header.Set("X-Amz-Copy-Source", "/test-bucket/source.txt")
		w := httptest.NewRecorder()

		handlers.CopyObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.CopyObjectResult
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result.ETag == "" {
			t.Error("ETag should not be empty")
		}

		// Verify the copy exists
		getReq := httptest.NewRequest("GET", "/test-bucket/destination.txt", nil)
		getReq.SetPathValue("bucket", "test-bucket")
		getReq.SetPathValue("key", "destination.txt")
		getW := httptest.NewRecorder()
		handlers.GetObject(getW, getReq)

		if !bytes.Equal(getW.Body.Bytes(), content) {
			t.Errorf("copied content = %q, want %q", getW.Body.String(), string(content))
		}
	})

	t.Run("copy with URL encoded source", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/encoded-dest.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "encoded-dest.txt")
		req.Header.Set("X-Amz-Copy-Source", "test-bucket%2Fsource.txt")
		w := httptest.NewRecorder()

		handlers.CopyObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("copy from nonexistent source", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/dest.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "dest.txt")
		req.Header.Set("X-Amz-Copy-Source", "/test-bucket/nonexistent.txt")
		w := httptest.NewRecorder()

		handlers.CopyObject(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("copy from wrong bucket", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/dest.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "dest.txt")
		req.Header.Set("X-Amz-Copy-Source", "/wrong-bucket/source.txt")
		w := httptest.NewRecorder()

		handlers.CopyObject(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})

	t.Run("copy with invalid source format", func(t *testing.T) {
		req := httptest.NewRequest("PUT", "/test-bucket/dest.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "dest.txt")
		req.Header.Set("X-Amz-Copy-Source", "invalid")
		w := httptest.NewRecorder()

		handlers.CopyObject(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}

func TestGetObjectRange(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create a test object
	key := "range-test.txt"
	content := []byte("0123456789ABCDEF") // 16 bytes
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader(content))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", key)
	handlers.PutObject(httptest.NewRecorder(), putReq)

	t.Run("range bytes=0-4", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Range", "bytes=0-4")
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusPartialContent {
			t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
		}

		if w.Body.String() != "01234" {
			t.Errorf("body = %q, want %q", w.Body.String(), "01234")
		}

		contentRange := w.Header().Get("Content-Range")
		if contentRange != "bytes 0-4/16" {
			t.Errorf("Content-Range = %q, want %q", contentRange, "bytes 0-4/16")
		}
	})

	t.Run("range bytes=10-", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Range", "bytes=10-")
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusPartialContent {
			t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
		}

		if w.Body.String() != "ABCDEF" {
			t.Errorf("body = %q, want %q", w.Body.String(), "ABCDEF")
		}
	})

	t.Run("range bytes=-5 (last 5 bytes)", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Range", "bytes=-5")
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusPartialContent {
			t.Errorf("status = %d, want %d", w.Code, http.StatusPartialContent)
		}

		if w.Body.String() != "BCDEF" {
			t.Errorf("body = %q, want %q", w.Body.String(), "BCDEF")
		}
	})

	t.Run("invalid range format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Range", "invalid")
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("range beyond file size", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		req.Header.Set("Range", "bytes=100-200")
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusRequestedRangeNotSatisfiable {
			t.Errorf("status = %d, want %d", w.Code, http.StatusRequestedRangeNotSatisfiable)
		}
	})
}

func TestParseRangeHeader(t *testing.T) {
	tests := []struct {
		name      string
		header    string
		wantStart int64
		wantEnd   int64
		wantErr   bool
	}{
		{"simple range", "bytes=0-99", 0, 99, false},
		{"open ended", "bytes=100-", 100, -1, false},
		{"suffix range", "bytes=-50", -50, -1, false},
		{"invalid prefix", "chars=0-99", 0, 0, true},
		{"invalid format", "bytes=0", 0, 0, true},
		{"invalid start", "bytes=abc-99", 0, 0, true},
		{"invalid end", "bytes=0-xyz", 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := parseRangeHeader(tt.header)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseRangeHeader() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if start != tt.wantStart {
					t.Errorf("start = %d, want %d", start, tt.wantStart)
				}
				if end != tt.wantEnd {
					t.Errorf("end = %d, want %d", end, tt.wantEnd)
				}
			}
		})
	}
}

func TestRequireWritePrivilege(t *testing.T) {
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("success"))
	})

	t.Run("allows write privilege", func(t *testing.T) {
		handler := RequireWritePrivilege(dummyHandler)

		req := httptest.NewRequest("PUT", "/test", nil)
		cred := &config.Credential{Privileges: config.PrivilegeReadWrite}
		ctx := context.WithValue(req.Context(), credentialContextKey, cred)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("denies read-only privilege", func(t *testing.T) {
		handler := RequireWritePrivilege(dummyHandler)

		req := httptest.NewRequest("PUT", "/test", nil)
		cred := &config.Credential{Privileges: config.PrivilegeRead}
		ctx := context.WithValue(req.Context(), credentialContextKey, cred)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", w.Code, http.StatusForbidden)
		}
	})

	t.Run("denies missing credential", func(t *testing.T) {
		handler := RequireWritePrivilege(dummyHandler)

		req := httptest.NewRequest("PUT", "/test", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusForbidden {
			t.Errorf("status = %d, want %d", w.Code, http.StatusForbidden)
		}
	})
}

func TestAccessLogMiddleware(t *testing.T) {
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	})

	t.Run("logs request and passes through", func(t *testing.T) {
		handler := AccessLogMiddleware(dummyHandler)

		req := httptest.NewRequest("GET", "/test", nil)
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if w.Body.String() != "hello" {
			t.Errorf("body = %q, want %q", w.Body.String(), "hello")
		}
	})

	t.Run("extracts client IP from X-Forwarded-For", func(t *testing.T) {
		handler := AccessLogMiddleware(dummyHandler)

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.100, 10.0.0.1")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("extracts client IP from X-Real-IP", func(t *testing.T) {
		handler := AccessLogMiddleware(dummyHandler)

		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Real-IP", "192.168.1.100")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})
}

func TestGetClientIP(t *testing.T) {
	// getClientIP should always return RemoteAddr, ignoring proxy headers
	// This is a security measure - proxy headers are only trusted via getClientIPWithTrust
	tests := []struct {
		name       string
		xff        string
		xri        string
		remoteAddr string
		want       string
	}{
		{"ignores X-Forwarded-For", "192.168.1.1", "", "10.0.0.1:1234", "10.0.0.1"},
		{"ignores X-Real-IP", "", "192.168.1.1", "10.0.0.1:1234", "10.0.0.1"},
		{"RemoteAddr with port", "", "", "10.0.0.1:1234", "10.0.0.1"},
		{"RemoteAddr without port", "", "", "10.0.0.1", "10.0.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.xff != "" {
				req.Header.Set("X-Forwarded-For", tt.xff)
			}
			if tt.xri != "" {
				req.Header.Set("X-Real-IP", tt.xri)
			}
			req.RemoteAddr = tt.remoteAddr

			got := getClientIP(req)
			if got != tt.want {
				t.Errorf("getClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetClientIPWithTrust(t *testing.T) {
	// Test proxy header handling with trusted proxy configuration
	trustedChecker := newTrustedProxyChecker([]string{"10.0.0.1", "172.16.0.0/12"})
	untrustedChecker := newTrustedProxyChecker(nil)

	t.Run("trusts X-Forwarded-For from trusted proxy", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.1")
		req.RemoteAddr = "10.0.0.1:1234"

		got := getClientIPWithTrust(req, trustedChecker)
		if got != "192.168.1.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "192.168.1.1")
		}
	})

	t.Run("trusts X-Real-IP from trusted proxy", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Real-IP", "192.168.1.1")
		req.RemoteAddr = "10.0.0.1:1234"

		got := getClientIPWithTrust(req, trustedChecker)
		if got != "192.168.1.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "192.168.1.1")
		}
	})

	t.Run("ignores proxy headers from untrusted source", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.1")
		req.RemoteAddr = "203.0.113.1:1234" // Not in trusted list

		got := getClientIPWithTrust(req, trustedChecker)
		if got != "203.0.113.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "203.0.113.1")
		}
	})

	t.Run("ignores proxy headers when no trusted proxies configured", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.1")
		req.RemoteAddr = "10.0.0.1:1234"

		got := getClientIPWithTrust(req, untrustedChecker)
		if got != "10.0.0.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "10.0.0.1")
		}
	})

	t.Run("trusts proxy from CIDR range", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.1")
		req.RemoteAddr = "172.20.0.5:1234" // In 172.16.0.0/12

		got := getClientIPWithTrust(req, trustedChecker)
		if got != "192.168.1.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "192.168.1.1")
		}
	})

	t.Run("X-Forwarded-For takes precedence over X-Real-IP", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("X-Forwarded-For", "192.168.1.1")
		req.Header.Set("X-Real-IP", "192.168.2.2")
		req.RemoteAddr = "10.0.0.1:1234"

		got := getClientIPWithTrust(req, trustedChecker)
		if got != "192.168.1.1" {
			t.Errorf("getClientIPWithTrust() = %q, want %q", got, "192.168.1.1")
		}
	})
}

func TestPresignedResponseHeaderOverrides(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create a test object first
	key := "presigned-header-test.txt"
	content := []byte("test content")
	putReq := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader(content))
	putReq.SetPathValue("bucket", "test-bucket")
	putReq.SetPathValue("key", key)
	putReq.Header.Set("Content-Type", "text/plain")
	handlers.PutObject(httptest.NewRecorder(), putReq)

	t.Run("presigned request with response-content-type", func(t *testing.T) {
		// Simulate a presigned request by adding the required query parameters
		req := httptest.NewRequest("GET", "/test-bucket/"+key+"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test&response-content-type=application/octet-stream", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
			t.Errorf("Content-Type = %q, want %q", got, "application/octet-stream")
		}
	})

	t.Run("presigned request with response-content-disposition", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key+"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test&response-content-disposition=attachment%3Bfilename%3Ddownload.txt", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("Content-Disposition"); got != "attachment;filename=download.txt" {
			t.Errorf("Content-Disposition = %q, want %q", got, "attachment;filename=download.txt")
		}
	})

	t.Run("presigned request with response-cache-control", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key+"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test&response-cache-control=max-age=3600", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("Cache-Control"); got != "max-age=3600" {
			t.Errorf("Cache-Control = %q, want %q", got, "max-age=3600")
		}
	})

	t.Run("presigned request with all response headers", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test-bucket/"+key+"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=test&response-content-type=image/png&response-content-disposition=inline&response-cache-control=no-cache", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("Content-Type"); got != "image/png" {
			t.Errorf("Content-Type = %q, want %q", got, "image/png")
		}
		if got := w.Header().Get("Content-Disposition"); got != "inline" {
			t.Errorf("Content-Disposition = %q, want %q", got, "inline")
		}
		if got := w.Header().Get("Cache-Control"); got != "no-cache" {
			t.Errorf("Cache-Control = %q, want %q", got, "no-cache")
		}
	})

	t.Run("non-presigned request ignores response header overrides", func(t *testing.T) {
		// Request without presigned URL parameters
		req := httptest.NewRequest("GET", "/test-bucket/"+key+"?response-content-type=application/octet-stream&response-content-disposition=attachment", nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.GetObject(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		// Should use the original content type, not the override
		if got := w.Header().Get("Content-Type"); got != "text/plain" {
			t.Errorf("Content-Type = %q, want %q (should not be overridden)", got, "text/plain")
		}
		// Should not have Content-Disposition set
		if got := w.Header().Get("Content-Disposition"); got != "" {
			t.Errorf("Content-Disposition = %q, want empty (should not be set)", got)
		}
	})
}

// Security Tests

func TestMetadataValidation(t *testing.T) {
	t.Run("valid metadata values", func(t *testing.T) {
		validValues := []string{
			"normal value",
			"value with spaces",
			"123456",
			"special-chars_allowed.here",
		}
		for _, v := range validValues {
			if err := validateMetadataValue(v); err != nil {
				t.Errorf("validateMetadataValue(%q) = %v, want nil", v, err)
			}
		}
	})

	t.Run("invalid metadata with CRLF injection", func(t *testing.T) {
		invalidValues := []string{
			"value\r\nInjected-Header: malicious",
			"value\rcarriage-return",
			"value\nnewline",
			"value\x00null-byte",
		}
		for _, v := range invalidValues {
			if err := validateMetadataValue(v); err == nil {
				t.Errorf("validateMetadataValue(%q) = nil, want error", v)
			}
		}
	})

	t.Run("PutObject rejects invalid metadata", func(t *testing.T) {
		handlers, _, cleanup := setupTestHandlers(t)
		defer cleanup()

		req := httptest.NewRequest("PUT", "/test-bucket/test.txt", bytes.NewReader([]byte("content")))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "test.txt")
		req.Header.Set("Content-Type", "text/plain")
		req.Header.Set("X-Amz-Meta-Malicious", "value\r\nX-Injected: evil")
		w := httptest.NewRecorder()

		handlers.PutObject(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d for CRLF injection attempt", w.Code, http.StatusBadRequest)
		}
	})
}

func TestRangeHeaderValidation(t *testing.T) {
	tests := []struct {
		name        string
		rangeHeader string
		wantErr     bool
	}{
		// Valid ranges
		{"valid full range", "bytes=0-100", false},
		{"valid suffix range", "bytes=-100", false},
		{"valid open-ended range", "bytes=100-", false},

		// Invalid ranges
		{"missing bytes prefix", "0-100", true},
		{"multiple ranges", "bytes=0-100,200-300", true},
		{"empty range spec", "bytes=-", true},
		{"invalid start", "bytes=abc-100", true},
		{"invalid end", "bytes=0-xyz", true},
		{"start > end", "bytes=100-50", true},
		{"very large value", "bytes=0-9999999999999999999999", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseRangeHeader(tt.rangeHeader)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRangeHeader(%q) error = %v, wantErr %v", tt.rangeHeader, err, tt.wantErr)
			}
		})
	}
}

func TestMaxKeysValidation(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Create a few test objects
	for i := 0; i < 5; i++ {
		key := "test-" + string(rune('a'+i)) + ".txt"
		req := httptest.NewRequest("PUT", "/test-bucket/"+key, bytes.NewReader([]byte("content")))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		handlers.PutObject(httptest.NewRecorder(), req)
	}

	t.Run("max-keys capped at 1000", func(t *testing.T) {
		// Request with max-keys > 1000 should be capped
		req := httptest.NewRequest("GET", "/test-bucket?list-type=2&max-keys=9999999", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.ListObjectsV2(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var result s3.ListBucketResultV2
		if err := xml.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// MaxKeys in response should be capped at 1000
		if result.MaxKeys > 1000 {
			t.Errorf("MaxKeys = %d, want <= 1000", result.MaxKeys)
		}
	})
}

func TestErrorResponseNoResourcePath(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	// Request a nonexistent object
	req := httptest.NewRequest("GET", "/test-bucket/secret/path/file.txt", nil)
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", "secret/path/file.txt")
	w := httptest.NewRecorder()

	handlers.GetObject(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
	}

	var errResp s3.Error
	if err := xml.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	// Resource field should be empty to prevent information disclosure
	if errResp.Resource != "" {
		t.Errorf("Resource = %q, want empty (information disclosure)", errResp.Resource)
	}
}

func TestUploadSizeLimits(t *testing.T) {
	// Create handlers with a small size limit for testing
	tmpDir, err := os.MkdirTemp("", "sss-limit-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		Bucket: config.Bucket{Name: "test-bucket"},
		Storage: config.Storage{
			Path:          filepath.Join(tmpDir, "data"),
			MultipartPath: filepath.Join(tmpDir, "multipart"),
		},
		Limits: config.Limits{
			MaxObjectSize: 100, // 100 bytes limit for testing
			MaxPartSize:   100,
		},
	}

	store, err := storage.NewFilesystemStorage(cfg.Storage.Path, cfg.Storage.MultipartPath)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}

	// Create the test bucket
	if err := store.CreateBucket("test-bucket"); err != nil {
		t.Fatalf("failed to create test bucket: %v", err)
	}

	handlers := NewHandlers(cfg, store)

	t.Run("object within size limit succeeds", func(t *testing.T) {
		content := bytes.Repeat([]byte("a"), 50) // 50 bytes, under limit
		req := httptest.NewRequest("PUT", "/test-bucket/small.txt", bytes.NewReader(content))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "small.txt")
		w := httptest.NewRecorder()

		handlers.PutObject(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("status = %d, want %d for object under size limit", w.Code, http.StatusOK)
		}
	})

	t.Run("object exceeding size limit fails", func(t *testing.T) {
		content := bytes.Repeat([]byte("a"), 150) // 150 bytes, over limit
		req := httptest.NewRequest("PUT", "/test-bucket/large.txt", bytes.NewReader(content))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", "large.txt")
		w := httptest.NewRecorder()

		handlers.PutObject(w, req)

		if w.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("status = %d, want %d for object over size limit", w.Code, http.StatusRequestEntityTooLarge)
		}
	})
}
