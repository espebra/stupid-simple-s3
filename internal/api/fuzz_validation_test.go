package api

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
)

var fuzzMetadataKeyPattern = regexp.MustCompile(`^[a-z0-9-]+$`)

// FuzzExtractMetadata tests x-amz-meta-* header validation with arbitrary
// names and values. Accepted metadata must have keys of lowercase letters,
// digits and hyphens, and printable ASCII values equal to the header value.
//
// Run with: go test -fuzz=FuzzExtractMetadata -fuzztime=30s ./internal/api/
func FuzzExtractMetadata(f *testing.F) {
	f.Add("Color", "blue")
	f.Add("color", "")
	f.Add("", "v")
	f.Add("my_key", "v")
	f.Add("my.key", "v")
	f.Add("a b", "v")
	f.Add("Clé", "v")
	f.Add("K", "v") // Kelvin sign, lowercases to ASCII 'k'
	f.Add("key", "a\r\nX-Injected: evil")
	f.Add("key", "a\x00b")
	f.Add("key", "a\x7f")
	f.Add("key", "\t")
	f.Add("key", "café")
	f.Add("key", " !\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~")

	f.Fuzz(func(t *testing.T, name, value string) {
		// Use the map directly so the name is not canonicalized
		h := http.Header{"X-Amz-Meta-" + name: {value}}
		got, err := extractAndValidateMetadata(h)
		if err != nil {
			if got != nil {
				t.Fatalf("error %v with non-nil metadata %v", err, got)
			}
			return
		}
		if len(got) != 1 {
			t.Fatalf("accepted %q: %q with %d entries: %v", name, value, len(got), got)
		}
		for k, v := range got {
			if !fuzzMetadataKeyPattern.MatchString(k) {
				t.Fatalf("accepted key %q from name %q", k, name)
			}
			if v != value {
				t.Fatalf("value %q, want %q", v, value)
			}
			for i := 0; i < len(v); i++ {
				if v[i] < 0x20 || v[i] > 0x7e {
					t.Fatalf("accepted value %q with byte %#x", v, v[i])
				}
			}
		}
	})
}

// FuzzRedactedRequestURI tests that the access log never shows the value
// of an X-Amz-* query parameter.
//
// The check is made per &-separated parameter, as the server sees them:
// each raw parameter is parsed on its own with url.ParseQuery, and if it
// names an X-Amz-* parameter, its value must not be in the corresponding
// parameter of the output. A parameter that url.ParseQuery rejects
// (a ';' or an invalid escape) is not a query parameter to net/http, so it
// carries no credential and is not checked. Checking the value against the
// whole output instead would give false positives when the same text also
// appears in the path, in another parameter or in the parameter's name, so
// only the value part of the output parameter is searched; that the whole
// parameter becomes name=REDACTED is checked separately.
//
// Run with: go test -fuzz=FuzzRedactedRequestURI -fuzztime=30s ./internal/api/
func FuzzRedactedRequestURI(f *testing.F) {
	f.Add("X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20240101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=abcdef0123456789")
	f.Add("uploadId=abc&partNumber=1")
	f.Add("x-amz-signature=secret&X-AMZ-SIGNATURE=other")
	f.Add("x%2Damz-signature=secret")
	f.Add("X-Amz-Signature=sec%20ret+value")
	f.Add("X-Amz-Signature=a=b=c")
	f.Add("X-Amz-Signature")
	f.Add("X-Amz-Signature=secret;x=1")
	f.Add("X-Amz-Signature=%zzsecret")
	f.Add("X-Amz-%zz=secret")
	f.Add("&&X-Amz-Credential=secretvalue&")
	f.Add("x-amz-=secretvalue")
	f.Add("x-amz-secretvalue=secretvalue")

	f.Fuzz(func(t *testing.T, rawQuery string) {
		u := &url.URL{Path: "/b/k", RawQuery: rawQuery}
		out := redactedRequestURI(u)

		if rawQuery == "" {
			if out != "/b/k" {
				t.Fatalf("empty query gave %q", out)
			}
			return
		}
		path, outQuery, ok := strings.Cut(out, "?")
		if !ok || path != "/b/k" {
			t.Fatalf("output %q does not keep the path", out)
		}
		in := strings.Split(rawQuery, "&")
		outParams := strings.Split(outQuery, "&")
		if len(outParams) != len(in) {
			t.Fatalf("%d parameters in, %d out: %q -> %q", len(in), len(outParams), rawQuery, out)
		}

		for i, param := range in {
			values, err := url.ParseQuery(param)
			if err != nil {
				continue
			}
			for name, vs := range values {
				if len(name) < 6 || !strings.EqualFold(name[:6], "x-amz-") {
					// Other parameters are kept as they are
					if outParams[i] != param {
						t.Fatalf("parameter %q changed to %q", param, outParams[i])
					}
					continue
				}
				// Only the value part of the output parameter is checked: the
				// name is kept, so text of the value that is also in the name,
				// or spans the name and the '=', is not a leak
				escapedName := url.QueryEscape(name)
				_, outValue, _ := strings.Cut(outParams[i], "=")
				for _, v := range vs {
					if len(v) <= 3 || strings.Contains("REDACTED", v) {
						continue
					}
					if strings.Contains(outValue, v) || strings.Contains(outValue, url.QueryEscape(v)) {
						t.Fatalf("value %q of %q leaked: %q -> %q", v, name, rawQuery, out)
					}
				}
				if outParams[i] != escapedName+"=REDACTED" {
					t.Fatalf("parameter %q redacted to %q", param, outParams[i])
				}
			}
		}
	})
}

// FuzzCopySource tests CopyObject with arbitrary X-Amz-Copy-Source headers.
// The handler must never fail with a server error, and a successful copy
// must store the content of the object the decoded header names.
//
// Run with: go test -fuzz=FuzzCopySource -fuzztime=30s ./internal/api/
func FuzzCopySource(f *testing.F) {
	handlers, store, cleanup := setupTestHandlers(f)
	f.Cleanup(cleanup)

	if err := store.CreateBucket("dst-bucket"); err != nil {
		f.Fatalf("CreateBucket: %v", err)
	}
	for key, data := range map[string]string{
		"src.txt":       "source content",
		"dir/a b+ü.txt": "nested content",
		"%41":           "literal percent key",
		"empty":         "",
		"dir/":          "trailing slash key",
	} {
		if _, err := store.PutObject("test-bucket", key, "text/plain", nil, strings.NewReader(data)); err != nil {
			f.Fatalf("PutObject(%q): %v", key, err)
		}
	}

	f.Add("/test-bucket/src.txt")
	f.Add("test-bucket/src.txt")
	f.Add("test-bucket%2Fsrc.txt")
	f.Add("/test-bucket/dir/a%20b+%C3%BC.txt")
	f.Add("/test-bucket/%2541")
	f.Add("/test-bucket/%41")
	f.Add("/test-bucket/empty")
	f.Add("/test-bucket/dir/")
	f.Add("/test-bucket/")
	f.Add("/test-bucket/%zz")
	f.Add("/test-bucket/../src.txt")
	f.Add("/test-bucket//src.txt")
	f.Add("/test-bucket/src.txt/x")
	f.Add("/dst-bucket/dst-key")
	f.Add("/no-such-bucket/src.txt")
	f.Add("/Bad_Bucket/src.txt")
	f.Add("noslash")
	f.Add("/")
	f.Add("/test-bucket/src.txt?versionId=1")

	f.Fuzz(func(t *testing.T, src string) {
		// Header values cannot hold these; net/http rejects them on the wire.
		// An empty header is not a copy.
		if src == "" || strings.ContainsAny(src, "\r\n\x00") {
			return
		}

		// The object the header names, read before the copy, as the copy
		// may replace it when it names the destination
		var want []byte
		wantOK := false
		if decoded, err := url.PathUnescape(src); err == nil {
			if b, k, ok := strings.Cut(strings.TrimPrefix(decoded, "/"), "/"); ok {
				if r, _, err := store.GetObject(b, k); err == nil {
					want, err = io.ReadAll(r)
					_ = r.Close()
					wantOK = err == nil
				}
			}
		}

		req := httptest.NewRequest("PUT", "/dst-bucket/dst-key", nil)
		req.SetPathValue("bucket", "dst-bucket")
		req.SetPathValue("key", "dst-key")
		req.Header.Set("X-Amz-Copy-Source", src)
		w := httptest.NewRecorder()

		handlers.PutObject(w, req)

		switch w.Code {
		case http.StatusOK:
			if !wantOK {
				t.Fatalf("copy of %q succeeded, but it names no readable object", src)
			}
			r, _, err := store.GetObject("dst-bucket", "dst-key")
			if err != nil {
				t.Fatalf("copy of %q succeeded, but destination is unreadable: %v", src, err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil {
				t.Fatalf("reading destination: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("copy of %q stored %q, want %q", src, got, want)
			}
		case http.StatusBadRequest, http.StatusNotFound:
			if wantOK {
				t.Fatalf("copy of existing object %q failed with %d: %s", src, w.Code, w.Body.String())
			}
		default:
			t.Fatalf("copy of %q: status %d: %s", src, w.Code, w.Body.String())
		}
	})
}
