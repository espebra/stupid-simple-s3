package api

import (
	"bytes"
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
func setupTestHandlers(t *testing.T) (*Handlers, *config.Config, func()) {
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

	handlers := NewHandlers(cfg, store)

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return handlers, cfg, cleanup
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
