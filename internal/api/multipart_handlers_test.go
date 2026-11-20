package api

import (
	"bufio"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// mpRequest builds a request for a multipart handler with the path values set
func mpRequest(method, bucket, key, query string, body io.Reader) *http.Request {
	target := "/" + bucket + "/" + key
	if query != "" {
		target += "?" + query
	}
	req := httptest.NewRequest(method, target, body)
	req.SetPathValue("bucket", bucket)
	req.SetPathValue("key", key)
	return req
}

// mpNewUpload creates a multipart upload in test-bucket through the storage
func mpNewUpload(t *testing.T, store storage.MultipartStorage, key string) string {
	t.Helper()
	uploadID, err := store.CreateMultipartUpload("test-bucket", key, "text/plain", nil)
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}
	return uploadID
}

// mpUploadPart uploads a part through the storage and returns its ETag
func mpUploadPart(t *testing.T, store storage.MultipartStorage, uploadID string, partNumber int, data string) string {
	t.Helper()
	meta, err := store.UploadPart(uploadID, partNumber, strings.NewReader(data))
	if err != nil {
		t.Fatalf("UploadPart %d: %v", partNumber, err)
	}
	return meta.ETag
}

// mpCompleteXML builds a CompleteMultipartUpload body from part number and ETag pairs
func mpCompleteXML(parts ...any) string {
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for i := 0; i+1 < len(parts); i += 2 {
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", parts[i], parts[i+1])
	}
	b.WriteString("</CompleteMultipartUpload>")
	return b.String()
}

// mpUnexpectedEOFReader returns its data and then io.ErrUnexpectedEOF, as the
// HTTP server's body reader does when the client sends less than its
// Content-Length
type mpUnexpectedEOFReader struct{ r io.Reader }

func (u *mpUnexpectedEOFReader) Read(p []byte) (int, error) {
	n, err := u.r.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

func TestUploadPartErrors(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()
	if err := store.CreateBucket("other-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	const key = "part-errors.bin"
	uploadID := mpNewUpload(t, store, key)

	upload := func(bucket, k, query string, body io.Reader) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handlers.UploadPart(w, mpRequest("PUT", bucket, k, query, body))
		return w
	}

	t.Run("missing bucket", func(t *testing.T) {
		w := upload("no-such-bucket", key, "partNumber=1&uploadId="+uploadID, strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchBucket)
	})

	t.Run("missing uploadId", func(t *testing.T) {
		w := upload("test-bucket", key, "partNumber=1", strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	t.Run("empty uploadId", func(t *testing.T) {
		w := upload("test-bucket", key, "partNumber=1&uploadId=", strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	t.Run("missing partNumber", func(t *testing.T) {
		w := upload("test-bucket", key, "uploadId="+uploadID, strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	for _, pn := range []string{"0", "-1", "10001", "abc", "%201", "1.5", "0x1", "99999999999999999999"} {
		t.Run("partNumber "+pn, func(t *testing.T) {
			w := upload("test-bucket", key, "partNumber="+pn+"&uploadId="+uploadID, strings.NewReader("data"))
			assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
		})
	}

	for _, pn := range []int{1, 10000} {
		t.Run(fmt.Sprintf("partNumber %d accepted", pn), func(t *testing.T) {
			w := upload("test-bucket", key, fmt.Sprintf("partNumber=%d&uploadId=%s", pn, uploadID), strings.NewReader("data"))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d %s, want 200", w.Code, w.Body.String())
			}
			if w.Header().Get("ETag") == "" {
				t.Error("no ETag in response")
			}
		})
	}

	t.Run("unknown uploadId", func(t *testing.T) {
		w := upload("test-bucket", key, "partNumber=1&uploadId=00000000-0000-4000-8000-000000000000", strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("uploadId that is not a UUID", func(t *testing.T) {
		w := upload("test-bucket", key, "partNumber=1&uploadId=../../etc", strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("uploadId from another bucket", func(t *testing.T) {
		otherID, err := store.CreateMultipartUpload("other-bucket", key, "text/plain", nil)
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		w := upload("test-bucket", key, "partNumber=1&uploadId="+otherID, strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
		if parts, _ := store.ListParts(otherID); len(parts) != 0 {
			t.Errorf("upload in the other bucket has %d parts, want 0", len(parts))
		}
	})

	t.Run("key differs from the upload's key", func(t *testing.T) {
		w := upload("test-bucket", "other-key", "partNumber=2&uploadId="+uploadID, strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	t.Run("upload aborted before the part", func(t *testing.T) {
		abortedID := mpNewUpload(t, store, key)
		if err := store.AbortMultipartUpload(abortedID); err != nil {
			t.Fatalf("AbortMultipartUpload: %v", err)
		}
		w := upload("test-bucket", key, "partNumber=1&uploadId="+abortedID, strings.NewReader("data"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("invalid checksum header", func(t *testing.T) {
		req := mpRequest("PUT", "test-bucket", key, "partNumber=3&uploadId="+uploadID, strings.NewReader("data"))
		req.Header.Set("x-amz-checksum-sha256", "not base64!")
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidRequest)
	})

	t.Run("trailer checksum mismatch", func(t *testing.T) {
		body := encodeAWSChunkedWithTrailer([]byte("part data"), "x-amz-checksum-sha256:"+sha256Checksum("other data"))
		req := mpRequest("PUT", "test-bucket", key, "partNumber=4&uploadId="+uploadID, strings.NewReader(body))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		req.Header.Set("X-Amz-Trailer", "x-amz-checksum-sha256")
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrBadDigest)
	})

	t.Run("aws-chunked body without final chunk", func(t *testing.T) {
		req := mpRequest("PUT", "test-bucket", key, "partNumber=5&uploadId="+uploadID, strings.NewReader("5;chunk-signature=abc\r\nhello\r\n"))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrIncompleteBody)
	})

	t.Run("aws-chunked body with malformed chunk size", func(t *testing.T) {
		req := mpRequest("PUT", "test-bucket", key, "partNumber=6&uploadId="+uploadID, strings.NewReader("zz;chunk-signature=abc\r\nhello\r\n0\r\n\r\n"))
		req.Header.Set("Content-Encoding", "aws-chunked")
		req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidRequest)
	})

	t.Run("truncated body", func(t *testing.T) {
		req := mpRequest("PUT", "test-bucket", key, "partNumber=7&uploadId="+uploadID, &mpUnexpectedEOFReader{r: strings.NewReader("short")})
		req.ContentLength = 100
		w := httptest.NewRecorder()
		handlers.UploadPart(w, req)
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrIncompleteBody)
	})

	// None of the failed requests stored a part
	parts, err := store.ListParts(uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	for _, p := range parts {
		if p.PartNumber != 1 && p.PartNumber != 10000 {
			t.Errorf("failed request stored part %d", p.PartNumber)
		}
	}
}

// TestMalformedChunkIsClientError tests that an aws-chunked body that does
// not follow the encoding is answered with 400 InvalidRequest, not 500, by
// both UploadPart and PutObject
func TestMalformedChunkIsClientError(t *testing.T) {
	bodies := map[string]string{
		"chunk size not hex":    "zz;chunk-signature=abc\r\nhello\r\n0\r\n\r\n",
		"negative chunk size":   "-5\r\nhello\r\n0\r\n\r\n",
		"chunk line too long":   strings.Repeat("f", 2*maxChunkLineLength) + "\r\n",
		"trailer line too long": "5\r\nhello\r\n0\r\nx-amz-checksum-sha256:" + strings.Repeat("a", 2*maxChunkLineLength),
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			handlers, store, cleanup := setupTestHandlers(t)
			defer cleanup()
			uploadID := mpNewUpload(t, store, "malformed")

			req := mpRequest("PUT", "test-bucket", "malformed", "partNumber=1&uploadId="+uploadID, strings.NewReader(body))
			req.Header.Set("Content-Encoding", "aws-chunked")
			req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			w := httptest.NewRecorder()
			handlers.UploadPart(w, req)
			assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidRequest)

			req = mpRequest("PUT", "test-bucket", "malformed", "", strings.NewReader(body))
			req.Header.Set("Content-Encoding", "aws-chunked")
			req.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			w = httptest.NewRecorder()
			handlers.PutObject(w, req)
			assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidRequest)
			if _, err := store.HeadObject("test-bucket", "malformed"); err == nil {
				t.Error("object stored from a malformed body")
			}
		})
	}
}

func TestUploadPartEntityTooLarge(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()
	handlers.cfg.Limits.MaxPartSize = 10
	uploadID := mpNewUpload(t, store, "big")

	w := httptest.NewRecorder()
	handlers.UploadPart(w, mpRequest("PUT", "test-bucket", "big", "partNumber=1&uploadId="+uploadID, strings.NewReader("12345678901")))
	assertErrorCode(t, w, http.StatusRequestEntityTooLarge, s3.ErrEntityTooLarge)

	w = httptest.NewRecorder()
	handlers.UploadPart(w, mpRequest("PUT", "test-bucket", "big", "partNumber=2&uploadId="+uploadID, strings.NewReader("1234567890")))
	if w.Code != http.StatusOK {
		t.Errorf("part at the size limit: status = %d %s, want 200", w.Code, w.Body.String())
	}

	parts, err := store.ListParts(uploadID)
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts) != 1 || parts[0].PartNumber != 2 {
		t.Errorf("parts = %+v, want only part 2", parts)
	}
}

// TestUploadPartTruncatedOverHTTP sends a part with a Content-Length larger
// than the body through a real HTTP server and closes the connection
func TestUploadPartTruncatedOverHTTP(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()
	uploadID := mpNewUpload(t, store, "truncated")

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /{bucket}/{key...}", handlers.UploadPart)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	_, err = fmt.Fprintf(conn, "PUT /test-bucket/truncated?partNumber=1&uploadId=%s HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\nonly a few bytes", uploadID)
	if err != nil {
		t.Fatalf("writing request: %v", err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("reading response: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), string(s3.ErrIncompleteBody)) {
		t.Errorf("response = %d %s, want 400 %s", resp.StatusCode, body, s3.ErrIncompleteBody)
	}
	if parts, _ := store.ListParts(uploadID); len(parts) != 0 {
		t.Errorf("truncated part was stored: %+v", parts)
	}
}

func TestCreateMultipartUploadErrors(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	create := func(bucket, key string, headers map[string]string) *httptest.ResponseRecorder {
		req := mpRequest("POST", bucket, key, "uploads", nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		handlers.CreateMultipartUpload(w, req)
		return w
	}

	t.Run("missing bucket", func(t *testing.T) {
		assertErrorCode(t, create("no-such-bucket", "key", nil), http.StatusNotFound, s3.ErrNoSuchBucket)
	})

	for _, key := range []string{strings.Repeat("k", 1025), `a\..\b`} {
		t.Run(fmt.Sprintf("invalid key %.10q", key), func(t *testing.T) {
			assertErrorCode(t, create("test-bucket", key, nil), http.StatusBadRequest, s3.ErrInvalidArgument)
		})
	}

	for name, headers := range map[string]map[string]string{
		"metadata value with CRLF":      {"X-Amz-Meta-Evil": "value\r\nX-Injected: yes"},
		"metadata value not ASCII":      {"X-Amz-Meta-Name": "caf\xc3\xa9"},
		"metadata key with underscore":  {"X-Amz-Meta-My_key": "value"},
		"metadata key with punctuation": {"X-Amz-Meta-A.b": "value"},
	} {
		t.Run(name, func(t *testing.T) {
			assertErrorCode(t, create("test-bucket", "key", headers), http.StatusBadRequest, s3.ErrInvalidArgument)
		})
	}

	uploads, err := store.ListMultipartUploads("test-bucket", "")
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(uploads) != 0 {
		t.Errorf("rejected requests created %d uploads", len(uploads))
	}
}

func TestCreateMultipartUploadPreservesMetadata(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	const key = "with-metadata.txt"
	req := mpRequest("POST", "test-bucket", key, "uploads", nil)
	req.Header.Set("Content-Type", "text/csv")
	req.Header.Set("X-Amz-Meta-Color", "blue")
	req.Header.Set("X-Amz-Meta-Owner-Id", "42")
	w := httptest.NewRecorder()
	handlers.CreateMultipartUpload(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("CreateMultipartUpload = %d %s", w.Code, w.Body.String())
	}
	var created s3.InitiateMultipartUploadResult
	if err := xml.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	etag := mpUploadPart(t, store, created.UploadID, 1, "a,b,c\n")
	w = httptest.NewRecorder()
	handlers.CompleteMultipartUpload(w, mpRequest("POST", "test-bucket", key, "uploadId="+created.UploadID, strings.NewReader(mpCompleteXML(1, etag))))
	if w.Code != http.StatusOK {
		t.Fatalf("CompleteMultipartUpload = %d %s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	handlers.HeadObject(w, mpRequest("HEAD", "test-bucket", key, "", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("HeadObject = %d", w.Code)
	}
	for header, want := range map[string]string{
		"Content-Type":        "text/csv",
		"X-Amz-Meta-Color":    "blue",
		"X-Amz-Meta-Owner-Id": "42",
	} {
		if got := w.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}

	t.Run("default content type", func(t *testing.T) {
		w := httptest.NewRecorder()
		handlers.CreateMultipartUpload(w, mpRequest("POST", "test-bucket", "no-type", "uploads", nil))
		var created s3.InitiateMultipartUploadResult
		if err := xml.NewDecoder(w.Body).Decode(&created); err != nil {
			t.Fatalf("decoding response: %v", err)
		}
		meta, err := store.GetMultipartUpload(created.UploadID)
		if err != nil {
			t.Fatalf("GetMultipartUpload: %v", err)
		}
		if meta.ContentType != "application/octet-stream" {
			t.Errorf("ContentType = %q, want application/octet-stream", meta.ContentType)
		}
	})
}

func TestCompleteMultipartUploadErrors(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	complete := func(bucket, key, uploadID, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handlers.CompleteMultipartUpload(w, mpRequest("POST", bucket, key, "uploadId="+uploadID, strings.NewReader(body)))
		return w
	}
	// Parts below the minimum size may only be last, so these tests use
	// single-part completions or rely on the order check that comes first
	newUploadWithParts := func(t *testing.T, key string, n int) (string, []string) {
		uploadID := mpNewUpload(t, store, key)
		var etags []string
		for i := 1; i <= n; i++ {
			etags = append(etags, mpUploadPart(t, store, uploadID, i, fmt.Sprintf("part %d", i)))
		}
		return uploadID, etags
	}
	assertUploadKept := func(t *testing.T, uploadID, key string) {
		t.Helper()
		if _, err := store.GetMultipartUpload(uploadID); err != nil {
			t.Errorf("upload is gone after a rejected completion: %v", err)
		}
		if exists, _ := store.ObjectExists("test-bucket", key); exists {
			t.Error("rejected completion created the object")
		}
	}

	t.Run("missing bucket", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "nb", 1)
		w := complete("no-such-bucket", "nb", uploadID, mpCompleteXML(1, etags[0]))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchBucket)
	})

	t.Run("missing uploadId", func(t *testing.T) {
		w := httptest.NewRecorder()
		handlers.CompleteMultipartUpload(w, mpRequest("POST", "test-bucket", "k", "uploadId=", strings.NewReader(mpCompleteXML(1, "x"))))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	t.Run("unknown uploadId", func(t *testing.T) {
		w := complete("test-bucket", "k", "00000000-0000-4000-8000-000000000000", mpCompleteXML(1, "x"))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("key differs from the upload's key", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "right-key", 1)
		w := complete("test-bucket", "wrong-key", uploadID, mpCompleteXML(1, etags[0]))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidArgument)
		assertUploadKept(t, uploadID, "right-key")
	})

	t.Run("parts out of order", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "order", 2)
		w := complete("test-bucket", "order", uploadID, mpCompleteXML(2, etags[1], 1, etags[0]))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidPartOrder)
		assertUploadKept(t, uploadID, "order")
	})

	t.Run("duplicate part numbers", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "dup", 1)
		w := complete("test-bucket", "dup", uploadID, mpCompleteXML(1, etags[0], 1, etags[0]))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidPartOrder)
		assertUploadKept(t, uploadID, "dup")
	})

	t.Run("unknown part", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "unknown-part", 1)
		w := complete("test-bucket", "unknown-part", uploadID, mpCompleteXML(2, etags[0]))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidPart)
		assertUploadKept(t, uploadID, "unknown-part")
	})

	t.Run("wrong ETag", func(t *testing.T) {
		uploadID, _ := newUploadWithParts(t, "wrong-etag", 1)
		w := complete("test-bucket", "wrong-etag", uploadID, mpCompleteXML(1, `"0123456789abcdef0123456789abcdef"`))
		assertErrorCode(t, w, http.StatusBadRequest, s3.ErrInvalidPart)
		assertUploadKept(t, uploadID, "wrong-etag")
	})

	t.Run("unquoted ETag accepted", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "unquoted", 1)
		w := complete("test-bucket", "unquoted", uploadID, mpCompleteXML(1, strings.Trim(etags[0], `"`)))
		if w.Code != http.StatusOK {
			t.Errorf("status = %d %s, want 200", w.Code, w.Body.String())
		}
	})

	for name, body := range map[string]string{
		"malformed XML":   "<CompleteMultipartUpload><Part><PartNumber>1",
		"not XML":         "hello",
		"empty body":      "",
		"empty part list": "<CompleteMultipartUpload></CompleteMultipartUpload>",
		"oversized body":  "<CompleteMultipartUpload>" + strings.Repeat(" ", 1024*1024) + "</CompleteMultipartUpload>",
	} {
		t.Run(name, func(t *testing.T) {
			key := strings.ReplaceAll(name, " ", "-")
			uploadID, _ := newUploadWithParts(t, key, 1)
			w := complete("test-bucket", key, uploadID, body)
			assertErrorCode(t, w, http.StatusBadRequest, s3.ErrMalformedXML)
			assertUploadKept(t, uploadID, key)
		})
	}

	t.Run("bucket deleted while the upload is in progress", func(t *testing.T) {
		if err := store.CreateBucket("doomed-bucket"); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		uploadID, err := store.CreateMultipartUpload("doomed-bucket", "k", "text/plain", nil)
		if err != nil {
			t.Fatalf("CreateMultipartUpload: %v", err)
		}
		etag := mpUploadPart(t, store, uploadID, 1, "data")
		// In-progress uploads do not keep a bucket from being deleted
		if err := store.DeleteBucket("doomed-bucket"); err != nil {
			t.Fatalf("DeleteBucket: %v", err)
		}
		w := complete("doomed-bucket", "k", uploadID, mpCompleteXML(1, etag))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchBucket)
	})

	t.Run("complete twice", func(t *testing.T) {
		uploadID, etags := newUploadWithParts(t, "twice", 1)
		if w := complete("test-bucket", "twice", uploadID, mpCompleteXML(1, etags[0])); w.Code != http.StatusOK {
			t.Fatalf("first completion = %d %s", w.Code, w.Body.String())
		}
		w := complete("test-bucket", "twice", uploadID, mpCompleteXML(1, etags[0]))
		assertErrorCode(t, w, http.StatusNotFound, s3.ErrNoSuchUpload)
	})
}

func TestAbortMultipartUploadErrors(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()
	if err := store.CreateBucket("other-bucket"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	abort := func(bucket, key, query string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handlers.AbortMultipartUpload(w, mpRequest("DELETE", bucket, key, query, nil))
		return w
	}
	assertUploadKept := func(t *testing.T, uploadID string) {
		t.Helper()
		if _, err := store.GetMultipartUpload(uploadID); err != nil {
			t.Errorf("upload is gone after a rejected abort: %v", err)
		}
	}

	t.Run("missing bucket", func(t *testing.T) {
		uploadID := mpNewUpload(t, store, "k")
		assertErrorCode(t, abort("no-such-bucket", "k", "uploadId="+uploadID), http.StatusNotFound, s3.ErrNoSuchBucket)
		assertUploadKept(t, uploadID)
	})

	t.Run("missing uploadId", func(t *testing.T) {
		assertErrorCode(t, abort("test-bucket", "k", "uploadId="), http.StatusBadRequest, s3.ErrInvalidArgument)
	})

	t.Run("unknown uploadId", func(t *testing.T) {
		assertErrorCode(t, abort("test-bucket", "k", "uploadId=00000000-0000-4000-8000-000000000000"), http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("uploadId that is not a UUID", func(t *testing.T) {
		assertErrorCode(t, abort("test-bucket", "k", "uploadId=..%2F..%2Fdata"), http.StatusNotFound, s3.ErrNoSuchUpload)
	})

	t.Run("uploadId from another bucket", func(t *testing.T) {
		uploadID := mpNewUpload(t, store, "k")
		assertErrorCode(t, abort("other-bucket", "k", "uploadId="+uploadID), http.StatusNotFound, s3.ErrNoSuchUpload)
		assertUploadKept(t, uploadID)
	})

	t.Run("uploadId for another key", func(t *testing.T) {
		uploadID := mpNewUpload(t, store, "k")
		assertErrorCode(t, abort("test-bucket", "other-key", "uploadId="+uploadID), http.StatusBadRequest, s3.ErrInvalidArgument)
		assertUploadKept(t, uploadID)
	})

	t.Run("double abort", func(t *testing.T) {
		uploadID := mpNewUpload(t, store, "k")
		mpUploadPart(t, store, uploadID, 1, "data")
		if w := abort("test-bucket", "k", "uploadId="+uploadID); w.Code != http.StatusNoContent {
			t.Fatalf("first abort = %d %s, want 204", w.Code, w.Body.String())
		}
		assertErrorCode(t, abort("test-bucket", "k", "uploadId="+uploadID), http.StatusNotFound, s3.ErrNoSuchUpload)
	})
}
