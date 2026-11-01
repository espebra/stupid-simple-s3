package api

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// FuzzRangeRequest tests GET requests with arbitrary Range headers against
// objects of several sizes. The handler must answer 206, 400 or 416. A 206
// must return exactly the bytes its Content-Range names, and a 416 must
// carry the object size in Content-Range.
//
// Run with: go test -fuzz=FuzzRangeRequest -fuzztime=30s ./internal/api/
func FuzzRangeRequest(f *testing.F) {
	handlers, store, cleanup := setupTestHandlers(f)
	f.Cleanup(cleanup)

	objects := map[string][]byte{}
	for _, size := range []int{0, 1, 100, 4097} {
		key := fmt.Sprintf("obj-%d", size)
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i * 7)
		}
		if _, err := store.PutObject("test-bucket", key, "application/octet-stream", nil, bytes.NewReader(data)); err != nil {
			f.Fatalf("PutObject: %v", err)
		}
		objects[key] = data
	}

	f.Add("bytes=0-0")
	f.Add("bytes=0-99")
	f.Add("bytes=50-")
	f.Add("bytes=-10")
	f.Add("bytes=-0")
	f.Add("bytes=0-")
	f.Add("bytes=99-50")
	f.Add("bytes=4096-4096")
	f.Add("bytes=1-2,3-4")
	f.Add("bytes=-")
	f.Add("bytes=+1-+2")
	f.Add("bytes=9223372036854775807-")
	f.Add("bytes=-9223372036854775807")
	f.Add("bytes=0-9223372036854775807")
	f.Add("bytes= 1-2")
	f.Add("items=0-1")
	f.Add("bytes=")

	f.Fuzz(func(t *testing.T, rangeHeader string) {
		// Header values cannot hold these; net/http rejects them on the wire
		if strings.ContainsAny(rangeHeader, "\r\n\x00") {
			return
		}
		for key, data := range objects {
			req := httptest.NewRequest("GET", "/test-bucket/"+key, nil)
			req.SetPathValue("bucket", "test-bucket")
			req.SetPathValue("key", key)
			req.Header.Set("Range", rangeHeader)
			w := httptest.NewRecorder()

			handlers.GetObjectRange(w, req)

			size := int64(len(data))
			switch w.Code {
			case http.StatusPartialContent:
				var start, end, total int64
				cr := w.Header().Get("Content-Range")
				if _, err := fmt.Sscanf(cr, "bytes %d-%d/%d", &start, &end, &total); err != nil {
					t.Fatalf("%s %q: bad Content-Range %q", key, rangeHeader, cr)
				}
				if total != size || start < 0 || start > end || end >= size {
					t.Fatalf("%s %q: Content-Range %q out of bounds for size %d", key, rangeHeader, cr, size)
				}
				if !bytes.Equal(w.Body.Bytes(), data[start:end+1]) {
					t.Fatalf("%s %q: body is %d bytes, want bytes %d-%d", key, rangeHeader, w.Body.Len(), start, end)
				}
				if cl := w.Header().Get("Content-Length"); cl != fmt.Sprint(end-start+1) {
					t.Fatalf("%s %q: Content-Length %s, want %d", key, rangeHeader, cl, end-start+1)
				}
			case http.StatusRequestedRangeNotSatisfiable:
				if cr := w.Header().Get("Content-Range"); cr != fmt.Sprintf("bytes */%d", size) {
					t.Fatalf("%s %q: 416 with Content-Range %q", key, rangeHeader, cr)
				}
			case http.StatusBadRequest:
			default:
				t.Fatalf("%s %q: status %d: %s", key, rangeHeader, w.Code, w.Body.String())
			}
		}
	})
}

// FuzzConditionalHeaders tests the evaluation of If-Match, If-None-Match,
// If-Modified-Since and If-Unmodified-Since. Evaluation must give 0, 304 or
// 412, a strong ETag comparison must never match where a weak one does not,
// and a conditional write must be refused with NotImplemented exactly when
// If-None-Match is set to something other than "*".
//
// Run with: go test -fuzz=FuzzConditionalHeaders -fuzztime=30s ./internal/api/
func FuzzConditionalHeaders(f *testing.F) {
	etag := `"sha256-0123abcd"`
	f.Add(etag, "", "", "", etag)
	f.Add("*", "", "", "", etag)
	f.Add("", "*", "", "", etag)
	f.Add("", " * ", "", "", etag)
	f.Add(`W/"sha256-0123abcd"`, `W/"sha256-0123abcd"`, "", "", etag)
	f.Add(`"a", "sha256-0123abcd"`, `"b",W/"c"`, "", "", etag)
	f.Add("", "", "Sun, 06 Nov 1994 08:49:37 GMT", "Sun, 06 Nov 1994 08:49:37 GMT", etag)
	f.Add("", "", "Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994", etag)
	f.Add("", "", "not a date", "", `"`)
	f.Add(",,,", "W/", "", "", `""`)
	f.Add(`""`, `W/""`, "", "", "")

	lastModified := time.Date(2024, 5, 6, 7, 8, 9, 500, time.UTC)

	f.Fuzz(func(t *testing.T, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, etag string) {
		for _, h := range []string{ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince} {
			if strings.ContainsAny(h, "\r\n\x00") {
				return
			}
		}

		if etagListMatches(ifMatch, etag, true) && !etagListMatches(ifMatch, etag, false) {
			t.Fatalf("etagListMatches(%q, %q): strong match but no weak match", ifMatch, etag)
		}
		if strings.TrimSpace(ifMatch) == "*" && !etagListMatches(ifMatch, etag, true) {
			t.Fatalf("etagListMatches(%q, %q): * did not match", ifMatch, etag)
		}

		req := httptest.NewRequest("GET", "/test-bucket/key", nil)
		for name, value := range map[string]string{
			"If-Match":            ifMatch,
			"If-None-Match":       ifNoneMatch,
			"If-Modified-Since":   ifModifiedSince,
			"If-Unmodified-Since": ifUnmodifiedSince,
		} {
			if value != "" {
				req.Header.Set(name, value)
			}
		}

		meta := &s3.ObjectMetadata{ETag: etag, LastModified: lastModified}
		switch status := checkPreconditions(req, meta); status {
		case 0, http.StatusNotModified, http.StatusPreconditionFailed:
		default:
			t.Fatalf("checkPreconditions returned %d", status)
		}

		cond, code := writeCondition(req)
		wantNotImplemented := ifNoneMatch != "" && strings.TrimSpace(ifNoneMatch) != "*"
		if (code == s3.ErrNotImplemented) != wantNotImplemented {
			t.Fatalf("writeCondition(If-None-Match %q) = %q", ifNoneMatch, code)
		}
		if code != "" {
			return
		}
		if (cond == nil) != (ifMatch == "" && ifNoneMatch == "") {
			t.Fatalf("writeCondition(%q, %q): condition is nil: %v", ifMatch, ifNoneMatch, cond == nil)
		}
		if cond == nil {
			return
		}
		// If-None-Match: * refuses to replace an existing object, and
		// If-Match needs an existing object
		if ifNoneMatch != "" && cond(meta) == nil {
			t.Fatalf("writeCondition(%q, %q) allowed replacing an object", ifMatch, ifNoneMatch)
		}
		if ifMatch != "" && cond(nil) == nil {
			t.Fatalf("writeCondition(%q, %q) allowed writing a missing object", ifMatch, ifNoneMatch)
		}
	})
}

// FuzzDeleteObjectsBody tests DeleteObjects with arbitrary request bodies.
// The handler must answer 200 or 400, and must never delete anything but
// objects in the request's bucket: an object in another bucket and a file
// next to the storage directory must survive every request.
//
// Run with: go test -fuzz=FuzzDeleteObjectsBody -fuzztime=30s ./internal/api/
func FuzzDeleteObjectsBody(f *testing.F) {
	handlers, store, cleanup := setupTestHandlers(f)
	f.Cleanup(cleanup)

	if err := store.CreateBucket("other-bucket"); err != nil {
		f.Fatalf("CreateBucket: %v", err)
	}
	if _, err := store.PutObject("other-bucket", "keep", "text/plain", nil, strings.NewReader("keep")); err != nil {
		f.Fatalf("PutObject: %v", err)
	}
	// The storage path is <tmp>/data, so this file is just outside it
	storagePath := handlers.cfg.Storage.Path
	sentinel := filepath.Join(filepath.Dir(storagePath), "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0600); err != nil {
		f.Fatalf("WriteFile: %v", err)
	}

	f.Add([]byte(`<Delete><Object><Key>a</Key></Object></Delete>`))
	f.Add([]byte(`<Delete><Quiet>true</Quiet><Object><Key>a</Key></Object><Object><Key>b/c</Key></Object></Delete>`))
	f.Add([]byte(`<Delete><Object><Key>../sentinel</Key></Object><Object><Key>../../sentinel</Key></Object></Delete>`))
	f.Add([]byte(`<Delete><Object><Key>../other-bucket/keep</Key></Object><Object><Key>/etc/passwd</Key></Object></Delete>`))
	f.Add([]byte(`<Delete><Object><Key>a\..\..\sentinel</Key></Object><Object><Key>a&#x0;b</Key></Object></Delete>`))
	f.Add([]byte(`<Delete><Object><Key></Key></Object><Object></Object></Delete>`))
	f.Add([]byte(`<?xml version="1.0" encoding="UTF-8"?><Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Object><Key>x</Key></Object></Delete>`))
	f.Add([]byte(`<Delete>`))
	f.Add([]byte(`<NotDelete/>`))
	f.Add([]byte(`<!DOCTYPE d [<!ENTITY e "x">]><Delete><Object><Key>&e;</Key></Object></Delete>`))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest("POST", "/test-bucket?delete", bytes.NewReader(body))
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()

		handlers.DeleteObjects(w, req)

		if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if _, err := os.Stat(sentinel); err != nil {
			t.Fatalf("file outside the storage directory is gone: %v", err)
		}
		if ok, err := store.ObjectExists("other-bucket", "keep"); !ok || err != nil {
			t.Fatalf("object in another bucket is gone: %v %v", ok, err)
		}
	})
}

// FuzzCompleteMultipartUploadBody tests CompleteMultipartUpload with
// arbitrary request bodies against an upload of two parts. The handler must
// answer 200 or 400, never 500, and a completed object must be the
// concatenation of the parts the body lists.
//
// Run with: go test -fuzz=FuzzCompleteMultipartUploadBody -fuzztime=30s ./internal/api/
func FuzzCompleteMultipartUploadBody(f *testing.F) {
	handlers, store, cleanup := setupTestHandlers(f)
	f.Cleanup(cleanup)

	const key = "multipart"
	parts := [][]byte{[]byte("part one"), []byte("part two")}
	var etags []string

	// newUpload creates an upload with both parts. Only the last part
	// listed may be smaller than the minimum part size, so a body that
	// completes the upload lists one part.
	newUpload := func(tb testing.TB) string {
		uploadID, err := store.CreateMultipartUpload("test-bucket", key, "text/plain", nil)
		if err != nil {
			tb.Fatalf("CreateMultipartUpload: %v", err)
		}
		etags = etags[:0]
		for i, p := range parts {
			meta, err := store.UploadPart(uploadID, i+1, bytes.NewReader(p))
			if err != nil {
				tb.Fatalf("UploadPart: %v", err)
			}
			etags = append(etags, meta.ETag)
		}
		return uploadID
	}
	uploadID := newUpload(f)

	part := func(n int, etag string) string {
		return fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", n, etag)
	}
	f.Add([]byte("<CompleteMultipartUpload>" + part(1, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(2, etags[1]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(1, etags[0]) + part(2, etags[1]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(2, etags[1]) + part(1, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(1, etags[0]) + part(1, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(1, `"wrong"`) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(3, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(0, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(-1, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>" + part(99999999, etags[0]) + "</CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload><Part><PartNumber>x</PartNumber></Part></CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload></CompleteMultipartUpload>"))
	f.Add([]byte("<CompleteMultipartUpload>"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest("POST", "/test-bucket/"+key+"?uploadId="+uploadID, bytes.NewReader(body))
		req.SetPathValue("bucket", "test-bucket")
		req.SetPathValue("key", key)
		w := httptest.NewRecorder()

		handlers.CompleteMultipartUpload(w, req)

		switch w.Code {
		case http.StatusOK:
			r, _, err := store.GetObject("test-bucket", key)
			if err != nil {
				t.Fatalf("GetObject after complete: %v", err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatalf("reading completed object: %v", err)
			}
			if !bytes.Equal(got, parts[0]) && !bytes.Equal(got, parts[1]) {
				t.Fatalf("completed object is %q, want one of the parts", got)
			}
			uploadID = newUpload(t)
		case http.StatusBadRequest:
		default:
			t.Fatalf("status %d for body %q: %s", w.Code, body, w.Body.String())
		}
	})
}
