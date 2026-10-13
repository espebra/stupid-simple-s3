package api

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// encodeAWSChunkedWithTrailer encodes data as a single unsigned aws-chunked
// chunk followed by the given trailer lines, as sent by AWS SDKs with
// STREAMING-UNSIGNED-PAYLOAD-TRAILER.
func encodeAWSChunkedWithTrailer(data []byte, trailers ...string) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%x\r\n", len(data))
	buf.Write(data)
	buf.WriteString("\r\n0\r\n")
	for _, t := range trailers {
		buf.WriteString(t + "\r\n")
	}
	buf.WriteString("\r\n")
	return buf.String()
}

func newPutRequest(key string, body string) *http.Request {
	req := httptest.NewRequest("PUT", "/test-bucket/"+key, strings.NewReader(body))
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", key)
	return req
}

func assertErrorCode(t *testing.T, w *httptest.ResponseRecorder, wantStatus int, wantCode s3.ErrorCode) {
	t.Helper()
	if w.Code != wantStatus {
		t.Errorf("status = %d, want %d", w.Code, wantStatus)
	}
	var errResp s3.Error
	_ = xml.NewDecoder(w.Body).Decode(&errResp)
	if errResp.Code != wantCode {
		t.Errorf("error code = %q, want %q", errResp.Code, wantCode)
	}
}

func TestAWSChunkedReaderTrailers(t *testing.T) {
	data := []byte("Hello, World!")

	t.Run("unsigned trailer", func(t *testing.T) {
		encoded := encodeAWSChunkedWithTrailer(data, "x-amz-checksum-crc32:7ErD0A==")
		reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
		got, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("got %q, want %q", got, data)
		}
		if v := reader.Trailer("X-Amz-Checksum-Crc32"); v != "7ErD0A==" {
			t.Errorf("trailer = %q, want %q", v, "7ErD0A==")
		}
	})

	t.Run("signed trailer", func(t *testing.T) {
		encoded := fmt.Sprintf("%x;chunk-signature=abc\r\n%s\r\n0;chunk-signature=def\r\n"+
			"x-amz-checksum-crc32:7ErD0A==\r\nx-amz-trailer-signature:ghi\r\n\r\n", len(data), data)
		reader := newAWSChunkedReader(strings.NewReader(encoded), 0)
		if _, err := io.ReadAll(reader); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if v := reader.Trailer("x-amz-checksum-crc32"); v != "7ErD0A==" {
			t.Errorf("trailer = %q, want %q", v, "7ErD0A==")
		}
		if v := reader.Trailer("x-amz-trailer-signature"); v != "ghi" {
			t.Errorf("trailer signature = %q, want %q", v, "ghi")
		}
	})

	t.Run("too many trailer lines", func(t *testing.T) {
		trailers := make([]string, maxTrailerLines+1)
		for i := range trailers {
			trailers[i] = fmt.Sprintf("x-trailer-%d:v", i)
		}
		reader := newAWSChunkedReader(strings.NewReader(encodeAWSChunkedWithTrailer(data, trailers...)), 0)
		if _, err := io.ReadAll(reader); err == nil {
			t.Error("expected error for too many trailer lines")
		}
	})
}

func TestPutObjectChecksum(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	content := "Hello, World!"
	checksum := storage.EncodeCRC32(crc32.ChecksumIEEE([]byte(content)))
	wrongChecksum := storage.EncodeCRC32(crc32.ChecksumIEEE([]byte("something else")))

	t.Run("no checksum sent", func(t *testing.T) {
		w := httptest.NewRecorder()
		handlers.PutObject(w, newPutRequest("none.txt", content))
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("x-amz-checksum-crc32"); got != checksum {
			t.Errorf("x-amz-checksum-crc32 = %q, want %q", got, checksum)
		}
		if got := w.Header().Get("x-amz-checksum-type"); got != "FULL_OBJECT" {
			t.Errorf("x-amz-checksum-type = %q, want FULL_OBJECT", got)
		}
	})

	t.Run("matching header", func(t *testing.T) {
		req := newPutRequest("header-ok.txt", content)
		req.Header.Set("x-amz-checksum-crc32", checksum)
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
	})

	t.Run("mismatching header", func(t *testing.T) {
		req := newPutRequest("header-bad.txt", content)
		req.Header.Set("x-amz-checksum-crc32", wrongChecksum)
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrBadDigest)
		if exists, _ := store.ObjectExists("test-bucket", "header-bad.txt"); exists {
			t.Error("object with bad checksum should not be stored")
		}
	})

	t.Run("malformed header", func(t *testing.T) {
		req := newPutRequest("header-malformed.txt", content)
		req.Header.Set("x-amz-checksum-crc32", "not-base64!")
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidRequest)
	})

	t.Run("matching trailer", func(t *testing.T) {
		req := newPutRequest("trailer-ok.txt", encodeAWSChunkedWithTrailer([]byte(content), "x-amz-checksum-crc32:"+checksum))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("x-amz-checksum-crc32"); got != checksum {
			t.Errorf("x-amz-checksum-crc32 = %q, want %q", got, checksum)
		}
	})

	t.Run("mismatching trailer", func(t *testing.T) {
		req := newPutRequest("trailer-bad.txt", encodeAWSChunkedWithTrailer([]byte(content), "x-amz-checksum-crc32:"+wrongChecksum))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrBadDigest)
		if exists, _ := store.ObjectExists("test-bucket", "trailer-bad.txt"); exists {
			t.Error("object with bad checksum should not be stored")
		}
	})

	t.Run("declared trailer missing", func(t *testing.T) {
		req := newPutRequest("trailer-missing.txt", encodeAWSChunkedWithTrailer([]byte(content)))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32")
		w := httptest.NewRecorder()
		handlers.PutObject(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrBadDigest)
	})
}

func TestGetObjectChecksumHeaders(t *testing.T) {
	handlers, _, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "checksum.txt"
	content := "0123456789"
	checksum := storage.EncodeCRC32(crc32.ChecksumIEEE([]byte(content)))
	handlers.PutObject(httptest.NewRecorder(), newPutRequest(key, content))

	newReq := func(method string) *http.Request {
		req := httptest.NewRequest(method, "/test-bucket/"+key, nil)
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		return req
	}

	t.Run("get", func(t *testing.T) {
		w := httptest.NewRecorder()
		handlers.GetObject(w, newReq("GET"))
		if got := w.Header().Get("x-amz-checksum-crc32"); got != checksum {
			t.Errorf("x-amz-checksum-crc32 = %q, want %q", got, checksum)
		}
		if got := w.Header().Get("x-amz-checksum-type"); got != "FULL_OBJECT" {
			t.Errorf("x-amz-checksum-type = %q, want FULL_OBJECT", got)
		}
	})

	t.Run("head", func(t *testing.T) {
		w := httptest.NewRecorder()
		handlers.HeadObject(w, newReq("HEAD"))
		if got := w.Header().Get("x-amz-checksum-crc32"); got != checksum {
			t.Errorf("x-amz-checksum-crc32 = %q, want %q", got, checksum)
		}
	})

	t.Run("range get has no checksum", func(t *testing.T) {
		req := newReq("GET")
		req.Header.Set("Range", "bytes=0-4")
		w := httptest.NewRecorder()
		handlers.GetObject(w, req)
		if w.Code != http.StatusPartialContent {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusPartialContent)
		}
		if got := w.Header().Get("x-amz-checksum-crc32"); got != "" {
			t.Errorf("range response should not have a checksum, got %q", got)
		}
	})
}

func TestUploadPartChecksum(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	key := "multipart-checksum.bin"
	uploadID, err := store.CreateMultipartUpload("test-bucket", key, "application/octet-stream", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	newPartRequest := func(partNumber int, body string) *http.Request {
		req := httptest.NewRequest("PUT", fmt.Sprintf("/test-bucket/%s?uploadId=%s&partNumber=%d", key, uploadID, partNumber), strings.NewReader(body))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		return req
	}

	content := "part data"
	checksum := storage.EncodeCRC32(crc32.ChecksumIEEE([]byte(content)))

	t.Run("matching header", func(t *testing.T) {
		req := newPartRequest(1, content)
		req.Header.Set("x-amz-checksum-crc32", checksum)
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}
		if got := w.Header().Get("x-amz-checksum-crc32"); got != checksum {
			t.Errorf("x-amz-checksum-crc32 = %q, want %q", got, checksum)
		}
	})

	t.Run("mismatching header", func(t *testing.T) {
		req := newPartRequest(2, content)
		req.Header.Set("x-amz-checksum-crc32", storage.EncodeCRC32(0))
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrBadDigest)
	})
}
