package api

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
	"github.com/espen/stupid-simple-s3/internal/storage"
)

// vtErrorCode decodes the S3 error code of an error response
func vtErrorCode(t *testing.T, w *httptest.ResponseRecorder) s3.ErrorCode {
	t.Helper()
	var e s3.Error
	if err := xml.Unmarshal(w.Body.Bytes(), &e); err != nil {
		t.Fatalf("decoding error response %q: %v", w.Body.String(), err)
	}
	return e.Code
}

// vtPut stores an object through the storage layer
func vtPut(t *testing.T, store storage.MultipartStorage, bucket, key, contentType string, meta map[string]string, data []byte) {
	t.Helper()
	if _, err := store.PutObject(bucket, key, contentType, meta, bytes.NewReader(data)); err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
}

// vtGet reads an object through the storage layer
func vtGet(t *testing.T, store storage.MultipartStorage, bucket, key string) ([]byte, *s3.ObjectMetadata) {
	t.Helper()
	r, meta, err := store.GetObject(bucket, key)
	if err != nil {
		t.Fatalf("GetObject(%s/%s): %v", bucket, key, err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("reading %s/%s: %v", bucket, key, err)
	}
	return data, meta
}

// vtCopyRequest builds a CopyObject request to test-bucket/dstKey
func vtCopyRequest(dstKey, source string) *http.Request {
	req := httptest.NewRequest("PUT", "/test-bucket/dst", nil)
	req.SetPathValue("bucket", "test-bucket")
	req.SetPathValue("key", dstKey)
	req.Header.Set("X-Amz-Copy-Source", source)
	return req
}

// invalidMetadataHeaders are x-amz-meta-* headers that must be rejected,
// keyed by header name as it appears in the request header map
var invalidMetadataHeaders = []struct {
	name, header, value string
}{
	{"underscore in key", "X-Amz-Meta-My_Key", "v"},
	{"dot in key", "X-Amz-Meta-My.Key", "v"},
	{"space in key", "X-Amz-Meta-My Key", "v"},
	{"empty key", "X-Amz-Meta-", "v"},
	{"non-ASCII key", "X-Amz-Meta-Clé", "v"},
	{"colon in key", "X-Amz-Meta-A:b", "v"},
	{"CR in value", "X-Amz-Meta-Key", "a\rb"},
	{"LF in value", "X-Amz-Meta-Key", "a\nb"},
	{"CRLF injection", "X-Amz-Meta-Key", "a\r\nX-Injected: evil"},
	{"NUL in value", "X-Amz-Meta-Key", "a\x00b"},
	{"DEL in value", "X-Amz-Meta-Key", "a\x7fb"},
	{"tab in value", "X-Amz-Meta-Key", "a\tb"},
	{"other control char in value", "X-Amz-Meta-Key", "a\x01b"},
	{"UTF-8 in value", "X-Amz-Meta-Key", "café"},
	{"high byte in value", "X-Amz-Meta-Key", "a\x80b"},
	{"0xff in value", "X-Amz-Meta-Key", "\xff"},
}

func TestExtractAndValidateMetadata(t *testing.T) {
	t.Run("rejects invalid keys and values", func(t *testing.T) {
		for _, tc := range invalidMetadataHeaders {
			t.Run(tc.name, func(t *testing.T) {
				h := http.Header{tc.header: {tc.value}}
				got, err := extractAndValidateMetadata(h)
				if !errors.Is(err, ErrInvalidMetadata) {
					t.Errorf("extractAndValidateMetadata(%q: %q) = %v, %v; want ErrInvalidMetadata", tc.header, tc.value, got, err)
				}
			})
		}
	})

	t.Run("lowercases keys", func(t *testing.T) {
		// The caller lowercases the name, so uppercase letters in the
		// header name are accepted and folded
		h := http.Header{"X-AMZ-META-MiXeD-Case-9": {"v"}}
		got, err := extractAndValidateMetadata(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if want := map[string]string{"mixed-case-9": "v"}; !mapsEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("accepts valid metadata", func(t *testing.T) {
		h := http.Header{
			"X-Amz-Meta-Author":  {"Jane Doe <jane@example.com>"},
			"X-Amz-Meta-Empty":   {""},
			"X-Amz-Meta-Symbols": {" !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"},
			"X-Amz-Meta-1-2-3":   {"first", "second"},
			"X-Amz-Meta--":       {"dash"},
			"Content-Type":       {"text/plain"},
			"X-Amz-Date":         {"20240101T000000Z"},
			"X-Amz-Metadata":     {"not metadata"},
		}
		got, err := extractAndValidateMetadata(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := map[string]string{
			"author":  "Jane Doe <jane@example.com>",
			"empty":   "",
			"symbols": " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
			"1-2-3":   "first", // only the first value is kept
			"-":       "dash",
		}
		if !mapsEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("header with no values is skipped", func(t *testing.T) {
		got, err := extractAndValidateMetadata(http.Header{"X-Amz-Meta-Bad_Key": {}})
		if err != nil || len(got) != 0 {
			t.Errorf("got %v, %v; want empty map, nil", got, err)
		}
	})
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func TestInvalidMetadataEndToEnd(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	vtPut(t, store, "test-bucket", "src", "text/plain", map[string]string{"orig": "1"}, []byte("source"))
	vtPut(t, store, "test-bucket", "existing-dst", "text/old", map[string]string{"old": "yes"}, []byte("old content"))

	for _, tc := range invalidMetadataHeaders {
		t.Run("PutObject/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest("PUT", "/test-bucket/put-target", bytes.NewReader([]byte("data")))
			req.SetPathValue("bucket", "test-bucket")
			req.SetPathValue("key", "put-target")
			req.Header[tc.header] = []string{tc.value}
			w := httptest.NewRecorder()

			handlers.PutObject(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
			if code := vtErrorCode(t, w); code != s3.ErrInvalidArgument {
				t.Errorf("code = %s, want %s", code, s3.ErrInvalidArgument)
			}
			if _, err := store.HeadObject("test-bucket", "put-target"); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("object was stored: HeadObject err = %v", err)
			}
		})

		t.Run("CopyObject REPLACE/"+tc.name, func(t *testing.T) {
			for _, dst := range []string{"copy-target", "existing-dst"} {
				req := vtCopyRequest(dst, "/test-bucket/src")
				req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
				req.Header[tc.header] = []string{tc.value}
				w := httptest.NewRecorder()

				// Through PutObject, as the router sends it
				handlers.PutObject(w, req)

				if w.Code != http.StatusBadRequest {
					t.Fatalf("%s: status = %d, want 400", dst, w.Code)
				}
				if code := vtErrorCode(t, w); code != s3.ErrInvalidArgument {
					t.Errorf("%s: code = %s, want %s", dst, code, s3.ErrInvalidArgument)
				}
			}
			if _, err := store.HeadObject("test-bucket", "copy-target"); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("copy was stored: HeadObject err = %v", err)
			}
			data, meta := vtGet(t, store, "test-bucket", "existing-dst")
			if string(data) != "old content" || meta.ContentType != "text/old" || !mapsEqual(meta.UserMetadata, map[string]string{"old": "yes"}) {
				t.Errorf("existing destination changed: %q %q %v", data, meta.ContentType, meta.UserMetadata)
			}
		})
	}
}

func TestCopyObjectValidation(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	srcMeta := map[string]string{"color": "blue"}
	vtPut(t, store, "test-bucket", "src.txt", "text/plain", srcMeta, []byte("plain source"))
	vtPut(t, store, "test-bucket", "dir/my file ü+1.txt", "text/x-special", srcMeta, []byte("special source"))

	errorCases := []struct {
		name      string
		dstKey    string
		source    string
		directive string
		status    int
		code      s3.ErrorCode
	}{
		{"unknown metadata directive", "d1", "/test-bucket/src.txt", "MERGE", 400, s3.ErrInvalidArgument},
		{"lowercase metadata directive", "d1", "/test-bucket/src.txt", "replace", 400, s3.ErrInvalidArgument},
		{"invalid destination key traversal", "../evil", "/test-bucket/src.txt", "", 400, s3.ErrInvalidArgument},
		{"invalid destination key absolute", "/abs", "/test-bucket/src.txt", "", 400, s3.ErrInvalidArgument},
		{"invalid percent escape", "d1", "/test-bucket/%zz", "", 400, s3.ErrInvalidArgument},
		{"truncated percent escape", "d1", "/test-bucket/src.txt%2", "", 400, s3.ErrInvalidArgument},
		{"no slash", "d1", "test-bucket", "", 400, s3.ErrInvalidArgument},
		{"leading slash only bucket", "d1", "/test-bucket", "", 400, s3.ErrInvalidArgument},
		{"empty source key", "d1", "/test-bucket/", "", 400, s3.ErrInvalidArgument},
		{"traversal source key", "d1", "/test-bucket/../x", "", 400, s3.ErrInvalidArgument},
		{"missing source bucket", "d1", "/no-such-bucket/src.txt", "", 404, s3.ErrNoSuchBucket},
		{"invalid source bucket name", "d1", "/Bad_Bucket/src.txt", "", 404, s3.ErrNoSuchBucket},
		{"missing source key", "d1", "/test-bucket/missing.txt", "", 404, s3.ErrNoSuchKey},
		{"missing key after decoding", "d1", "/test-bucket/dir/my%2Bfile", "", 404, s3.ErrNoSuchKey},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			req := vtCopyRequest(tc.dstKey, tc.source)
			if tc.directive != "" {
				req.Header.Set("X-Amz-Metadata-Directive", tc.directive)
			}
			w := httptest.NewRecorder()

			handlers.CopyObject(w, req)

			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
			if code := vtErrorCode(t, w); code != tc.code {
				t.Errorf("code = %s, want %s", code, tc.code)
			}
			if _, err := store.HeadObject("test-bucket", "d1"); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("destination was written: HeadObject err = %v", err)
			}
		})
	}

	t.Run("missing destination bucket", func(t *testing.T) {
		req := vtCopyRequest("d1", "/test-bucket/src.txt")
		req.SetPathValue("bucket", "no-such-bucket")
		w := httptest.NewRecorder()
		handlers.CopyObject(w, req)
		if w.Code != http.StatusNotFound || vtErrorCode(t, w) != s3.ErrNoSuchBucket {
			t.Errorf("status = %d, body %s; want 404 NoSuchBucket", w.Code, w.Body.String())
		}
	})

	successCases := []struct {
		name   string
		source string
		want   string
	}{
		{"leading slash", "/test-bucket/src.txt", "plain source"},
		{"no leading slash", "test-bucket/src.txt", "plain source"},
		{"encoded slash", "test-bucket%2Fsrc.txt", "plain source"},
		{"fully encoded with leading slash", "%2Ftest-bucket%2Fsrc.txt", "plain source"},
		{"encoded spaces and unicode", "/test-bucket/dir/my%20file%20%C3%BC%2B1.txt", "special source"},
		{"literal plus is kept", "/test-bucket/dir/my%20file%20%C3%BC+1.txt", "special source"},
		{"raw spaces and unicode", "test-bucket/dir/my file ü+1.txt", "special source"},
	}
	for i, tc := range successCases {
		t.Run(tc.name, func(t *testing.T) {
			dst := "copies/" + string(rune('a'+i))
			w := httptest.NewRecorder()
			handlers.CopyObject(w, vtCopyRequest(dst, tc.source))
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
			}
			var result s3.CopyObjectResult
			if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatalf("decoding result: %v", err)
			}
			data, meta := vtGet(t, store, "test-bucket", dst)
			if string(data) != tc.want {
				t.Errorf("content = %q, want %q", data, tc.want)
			}
			if result.ETag != meta.ETag {
				t.Errorf("result ETag %s, stored ETag %s", result.ETag, meta.ETag)
			}
			// The default directive (COPY) keeps the source's attributes
			if !mapsEqual(meta.UserMetadata, srcMeta) {
				t.Errorf("metadata = %v, want %v", meta.UserMetadata, srcMeta)
			}
		})
	}

	t.Run("REPLACE replaces content type and metadata", func(t *testing.T) {
		req := vtCopyRequest("replaced", "/test-bucket/src.txt")
		req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Meta-New", "value")
		w := httptest.NewRecorder()
		handlers.CopyObject(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", w.Code, w.Body.String())
		}
		data, meta := vtGet(t, store, "test-bucket", "replaced")
		if string(data) != "plain source" || meta.ContentType != "application/json" || !mapsEqual(meta.UserMetadata, map[string]string{"new": "value"}) {
			t.Errorf("got %q %q %v", data, meta.ContentType, meta.UserMetadata)
		}
	})

	t.Run("REPLACE without content type defaults it", func(t *testing.T) {
		req := vtCopyRequest("replaced-default", "/test-bucket/src.txt")
		req.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
		w := httptest.NewRecorder()
		handlers.CopyObject(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", w.Code, w.Body.String())
		}
		_, meta := vtGet(t, store, "test-bucket", "replaced-default")
		if meta.ContentType != "application/octet-stream" || len(meta.UserMetadata) != 0 {
			t.Errorf("got %q %v", meta.ContentType, meta.UserMetadata)
		}
	})

	t.Run("explicit COPY keeps attributes", func(t *testing.T) {
		req := vtCopyRequest("copied-explicit", "/test-bucket/src.txt")
		req.Header.Set("X-Amz-Metadata-Directive", "COPY")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Amz-Meta-Ignored", "x")
		w := httptest.NewRecorder()
		handlers.CopyObject(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", w.Code, w.Body.String())
		}
		_, meta := vtGet(t, store, "test-bucket", "copied-explicit")
		if meta.ContentType != "text/plain" || !mapsEqual(meta.UserMetadata, srcMeta) {
			t.Errorf("got %q %v", meta.ContentType, meta.UserMetadata)
		}
	})
}

func TestParseMaxKeys(t *testing.T) {
	tests := []struct {
		raw    string
		set    bool
		want   int
		wantOK bool
	}{
		{"", false, 1000, true},
		{"", true, 1000, true},
		{"0", true, 0, true},
		{"1", true, 1, true},
		{"5", true, 5, true},
		{"+5", true, 5, true},
		{"999", true, 999, true},
		{"1000", true, 1000, true},
		{"1001", true, 1000, true},
		{"2147483647", true, 1000, true},
		// Like S3, values that are not an integer from 0 to the largest
		// int32 are rejected
		{"2147483648", true, 0, false},
		{"9223372036854775807", true, 0, false},
		{"99999999999999999999999", true, 0, false},
		{"-1", true, 0, false},
		{"-9223372036854775808", true, 0, false},
		{"abc", true, 0, false},
		{"10abc", true, 0, false},
		{" 5", true, 0, false},
		{"5.0", true, 0, false},
		{"0x10", true, 0, false},
	}
	for _, tt := range tests {
		q := url.Values{}
		if tt.set {
			q.Set("max-keys", tt.raw)
		}
		got, ok := parseMaxKeys(q)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("parseMaxKeys(%q) = %d, %v, want %d, %v", tt.raw, got, ok, tt.want, tt.wantOK)
		}
	}
}

// TestListObjectsMaxKeys tests that both listing versions reject an invalid
// max-keys with InvalidArgument and list nothing for max-keys=0, as S3 does
func TestListObjectsMaxKeys(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()
	vtPut(t, store, "test-bucket", "a.txt", "text/plain", nil, []byte("a"))

	for _, listType := range []string{"", "list-type=2&"} {
		for _, bad := range []string{"-1", "abc", "2147483648"} {
			req := httptest.NewRequest("GET", "/test-bucket?"+listType+"max-keys="+bad, nil)
			req.SetPathValue("bucket", "test-bucket")
			w := httptest.NewRecorder()
			handlers.GetBucket(w, req)
			if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "<Code>InvalidArgument</Code>") {
				t.Errorf("%smax-keys=%s: status %d, body %s, want 400 InvalidArgument", listType, bad, w.Code, w.Body.String())
			}
		}

		req := httptest.NewRequest("GET", "/test-bucket?"+listType+"max-keys=0", nil)
		req.SetPathValue("bucket", "test-bucket")
		w := httptest.NewRecorder()
		handlers.GetBucket(w, req)
		body := w.Body.String()
		if w.Code != http.StatusOK {
			t.Fatalf("%smax-keys=0: status %d, body %s", listType, w.Code, body)
		}
		if strings.Contains(body, "<Contents>") || !strings.Contains(body, "<MaxKeys>0</MaxKeys>") || !strings.Contains(body, "<IsTruncated>false</IsTruncated>") {
			t.Errorf("%smax-keys=0: body %s, want no keys, MaxKeys 0 and IsTruncated false", listType, body)
		}

		req = httptest.NewRequest("GET", "/missing-bucket?"+listType+"max-keys=0", nil)
		req.SetPathValue("bucket", "missing-bucket")
		w = httptest.NewRecorder()
		handlers.GetBucket(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("%smax-keys=0 on a missing bucket: status %d, want 404", listType, w.Code)
		}
	}
}

func TestParseHTTPDate(t *testing.T) {
	want := time.Date(1994, time.November, 6, 8, 49, 37, 0, time.UTC)
	valid := []struct{ name, value string }{
		{"RFC 1123", "Sun, 06 Nov 1994 08:49:37 GMT"},
		{"RFC 850", "Sunday, 06-Nov-94 08:49:37 GMT"},
		{"ANSI C asctime", "Sun Nov  6 08:49:37 1994"},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseHTTPDate(tc.value)
			if !ok || !got.Equal(want) {
				t.Errorf("parseHTTPDate(%q) = %v, %v; want %v, true", tc.value, got, ok, want)
			}
		})
	}

	invalid := []string{
		"",
		"garbage",
		"1994-11-06T08:49:37Z",
		"Sun, 06 Nov 1994 08:49:37",
		"Sun, 32 Nov 1994 08:49:37 GMT",
		"Sun, 06 Nov 1994 25:49:37 GMT",
		"784111777",
		" Sun, 06 Nov 1994 08:49:37 GMT",
	}
	for _, v := range invalid {
		if got, ok := parseHTTPDate(v); ok {
			t.Errorf("parseHTTPDate(%q) = %v, true; want false", v, got)
		}
	}
}

func TestApplyResponseHeaderOverridesValidation(t *testing.T) {
	overrides := []struct{ param, header string }{
		{"response-content-type", "Content-Type"},
		{"response-content-disposition", "Content-Disposition"},
		{"response-cache-control", "Cache-Control"},
	}
	presigned := func(param, value string) *http.Request {
		q := url.Values{}
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Credential", "cred")
		q.Set("X-Amz-Signature", "sig")
		q.Set(param, value)
		return httptest.NewRequest("GET", "/test-bucket/key?"+q.Encode(), nil)
	}

	for _, o := range overrides {
		for _, bad := range []string{"a\rb", "a\nb", "a\r\nX-Injected: evil", "a\x00b", "\n", "\r"} {
			t.Run(o.param+"/rejects "+url.QueryEscape(bad), func(t *testing.T) {
				w := httptest.NewRecorder()
				w.Header().Set(o.header, "original")
				applyResponseHeaderOverrides(w, presigned(o.param, bad))
				if got := w.Header().Values(o.header); !slices.Equal(got, []string{"original"}) {
					t.Errorf("%s = %q, want [original]", o.header, got)
				}
				if got := w.Header().Get("X-Injected"); got != "" {
					t.Errorf("injected header set: %q", got)
				}
			})
		}

		for _, good := range []string{"text/plain; charset=utf-8", "attachment; filename=\"a b.txt\"", "max-age=60, public"} {
			t.Run(o.param+"/accepts "+url.QueryEscape(good), func(t *testing.T) {
				w := httptest.NewRecorder()
				w.Header().Set(o.header, "original")
				applyResponseHeaderOverrides(w, presigned(o.param, good))
				if got := w.Header().Get(o.header); got != good {
					t.Errorf("%s = %q, want %q", o.header, got, good)
				}
			})
		}

		t.Run(o.param+"/empty value is not applied", func(t *testing.T) {
			w := httptest.NewRecorder()
			w.Header().Set(o.header, "original")
			applyResponseHeaderOverrides(w, presigned(o.param, ""))
			if got := w.Header().Get(o.header); got != "original" {
				t.Errorf("%s = %q, want original", o.header, got)
			}
		})
	}

	t.Run("one bad value does not block the others", func(t *testing.T) {
		q := url.Values{}
		q.Set("X-Amz-Algorithm", "AWS4-HMAC-SHA256")
		q.Set("X-Amz-Credential", "cred")
		q.Set("X-Amz-Signature", "sig")
		q.Set("response-content-type", "bad\r\nvalue")
		q.Set("response-cache-control", "no-store")
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "original")
		applyResponseHeaderOverrides(w, httptest.NewRequest("GET", "/b/k?"+q.Encode(), nil))
		if w.Header().Get("Content-Type") != "original" || w.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("headers = %v", w.Header())
		}
	})
}

func TestTrustedProxyChecker(t *testing.T) {
	checker := newTrustedProxyChecker([]string{
		"10.0.0.0/8",
		" 192.168.1.10 ", // surrounding space is trimmed
		"2001:db8::1",
		"fd00::/64",
		"::ffff:172.16.0.1", // IPv4-mapped entry matches the IPv4 address
		"",
		"not-an-ip",
		"300.1.1.1",
		"10.0.0.0/33",
		"192.168.1.20:80",
		"fe80::1%eth0",
	})

	tests := []struct {
		ip   string
		want bool
	}{
		{"10.0.0.1", true},
		{"10.255.255.255", true},
		{"11.0.0.1", false},
		{"192.168.1.10", true},
		{"192.168.1.11", false},
		{"2001:db8::1", true},
		{"2001:0db8:0000:0000:0000:0000:0000:0001", true}, // non-canonical form
		{"2001:DB8::1", true},
		{"2001:db8::2", false},
		{"fd00::1234", true},
		{"fd00:0:0:1::1", false},
		{"172.16.0.1", true},
		{"::ffff:172.16.0.1", true},
		{"::ffff:10.1.2.3", true}, // IPv4-mapped form of an address in a v4 CIDR
		{"::ffff:192.168.1.10", true},
		{"::ffff:11.0.0.1", false},
		// Malformed entries were ignored
		{"192.168.1.20", false},
		{"fe80::1", false},
		// Malformed client addresses are never trusted
		{"", false},
		{"not-an-ip", false},
		{"10.0.0.1:1234", false},
		{"[2001:db8::1]", false},
		{"[2001:db8::1]:443", false},
		{"2001:db8::1%eth0", false},
		{" 10.0.0.1", false},
		{"10.0.0", false},
		{"010.000.000.001", false},
	}
	for _, tt := range tests {
		if got := checker.isTrusted(tt.ip); got != tt.want {
			t.Errorf("isTrusted(%q) = %v, want %v", tt.ip, got, tt.want)
		}
	}

	t.Run("no valid entries trusts nothing", func(t *testing.T) {
		c := newTrustedProxyChecker([]string{"bogus", "", "1.2.3.4/99"})
		for _, ip := range []string{"1.2.3.4", "127.0.0.1", "::1"} {
			if c.isTrusted(ip) {
				t.Errorf("isTrusted(%q) = true with no valid entries", ip)
			}
		}
	})
}

// TestGetClientIPWithTrustIPv6 checks that a trusted proxy connecting over
// IPv6 is recognized.
func TestGetClientIPWithTrustIPv6(t *testing.T) {
	checker := newTrustedProxyChecker([]string{"::1"})
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "[::1]:1234"
	req.Header.Set("X-Real-IP", "192.0.2.7")

	if got := getClientIP(req); got != "::1" {
		t.Errorf("getClientIP() = %q, want %q", got, "::1")
	}
	if got := getClientIPWithTrust(req, checker); got != "192.0.2.7" {
		t.Errorf("getClientIPWithTrust() = %q, want %q", got, "192.0.2.7")
	}
}

func TestListObjectsV1(t *testing.T) {
	handlers, store, cleanup := setupTestHandlers(t)
	defer cleanup()

	keys := []string{"a.txt", "b/1.txt", "b/2.txt", "c/1.txt", "d.txt"}
	for _, k := range keys {
		vtPut(t, store, "test-bucket", k, "text/plain", nil, []byte("data-"+k))
	}

	list := func(t *testing.T, bucket, rawQuery string) (*httptest.ResponseRecorder, s3.ListBucketResultV1) {
		t.Helper()
		target := "/" + bucket
		if rawQuery != "" {
			target += "?" + rawQuery
		}
		req := httptest.NewRequest("GET", target, nil)
		req.SetPathValue("bucket", bucket)
		w := httptest.NewRecorder()
		// Through GetBucket, which routes to ListObjectsV1 without list-type
		handlers.GetBucket(w, req)
		var result s3.ListBucketResultV1
		if w.Code == http.StatusOK {
			if err := xml.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatalf("decoding listing: %v", err)
			}
		}
		return w, result
	}
	contentKeys := func(r s3.ListBucketResultV1) []string {
		out := []string{}
		for _, o := range r.Contents {
			out = append(out, o.Key)
		}
		return out
	}
	prefixes := func(r s3.ListBucketResultV1) []string {
		out := []string{}
		for _, p := range r.CommonPrefixes {
			out = append(out, p.Prefix)
		}
		return out
	}

	tests := []struct {
		name         string
		query        string
		wantKeys     []string
		wantPrefixes []string
		truncated    bool
		nextMarker   string
		maxKeys      int
	}{
		{"all objects", "", keys, []string{}, false, "", 1000},
		{"prefix", "prefix=b%2F", []string{"b/1.txt", "b/2.txt"}, []string{}, false, "", 1000},
		{"prefix with no match", "prefix=zzz", []string{}, []string{}, false, "", 1000},
		{"delimiter", "delimiter=%2F", []string{"a.txt", "d.txt"}, []string{"b/", "c/"}, false, "", 1000},
		{"prefix and delimiter", "prefix=b%2F&delimiter=%2F", []string{"b/1.txt", "b/2.txt"}, []string{}, false, "", 1000},
		{"marker", "marker=b%2F1.txt", []string{"b/2.txt", "c/1.txt", "d.txt"}, []string{}, false, "", 1000},
		{"marker past the end", "marker=z", []string{}, []string{}, false, "", 1000},
		{"max-keys truncates without NextMarker", "max-keys=2", []string{"a.txt", "b/1.txt"}, []string{}, true, "", 2},
		{"max-keys equal to count", "max-keys=5", keys, []string{}, false, "", 5},
		{"max-keys with delimiter gives NextMarker", "delimiter=%2F&max-keys=2", []string{"a.txt"}, []string{"b/"}, true, "b/", 2},
		{"marker at common prefix skips its keys", "delimiter=%2F&marker=b%2F", []string{"d.txt"}, []string{"c/"}, false, "", 1000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, r := list(t, "test-bucket", tc.query)
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d: %s", w.Code, w.Body.String())
			}
			if got := contentKeys(r); !slices.Equal(got, tc.wantKeys) {
				t.Errorf("keys = %v, want %v", got, tc.wantKeys)
			}
			if got := prefixes(r); !slices.Equal(got, tc.wantPrefixes) {
				t.Errorf("common prefixes = %v, want %v", got, tc.wantPrefixes)
			}
			if r.IsTruncated != tc.truncated {
				t.Errorf("IsTruncated = %v, want %v", r.IsTruncated, tc.truncated)
			}
			if r.NextMarker != tc.nextMarker {
				t.Errorf("NextMarker = %q, want %q", r.NextMarker, tc.nextMarker)
			}
			if r.MaxKeys != tc.maxKeys {
				t.Errorf("MaxKeys = %d, want %d", r.MaxKeys, tc.maxKeys)
			}
			if r.Name != "test-bucket" {
				t.Errorf("Name = %q", r.Name)
			}
		})
	}

	t.Run("echoes request parameters", func(t *testing.T) {
		_, r := list(t, "test-bucket", "prefix=b&delimiter=%2F&marker=a")
		if r.Prefix != "b" || r.Delimiter != "/" || r.Marker != "a" {
			t.Errorf("Prefix %q Delimiter %q Marker %q", r.Prefix, r.Delimiter, r.Marker)
		}
	})

	t.Run("paging with marker visits every key once", func(t *testing.T) {
		var got []string
		marker := ""
		for range 10 {
			_, r := list(t, "test-bucket", "max-keys=2&marker="+url.QueryEscape(marker))
			page := contentKeys(r)
			got = append(got, page...)
			if !r.IsTruncated {
				break
			}
			// Without a delimiter, clients continue from the last key
			marker = page[len(page)-1]
		}
		if !slices.Equal(got, keys) {
			t.Errorf("paged keys = %v, want %v", got, keys)
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		w, _ := list(t, "no-such-bucket", "")
		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", w.Code)
		}
		if code := vtErrorCode(t, w); code != s3.ErrNoSuchBucket {
			t.Errorf("code = %s, want %s", code, s3.ErrNoSuchBucket)
		}
	})
}
