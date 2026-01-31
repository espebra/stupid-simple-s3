package integration

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// TestUnauthenticated_RequestsAreRejected verifies that requests without any
// authentication (no Authorization header, no presigned URL query parameters)
// are rejected with 403 Forbidden and an AccessDenied error.
func TestUnauthenticated_RequestsAreRejected(t *testing.T) {
	ts := NewTestServer(t)
	defer ts.Close()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		// Bucket operations
		{"HeadBucket", http.MethodHead, "/" + TestBucket, ""},
		{"GetBucket", http.MethodGet, "/" + TestBucket, ""},
		{"CreateBucket", http.MethodPut, "/new-bucket", ""},
		{"DeleteBucket", http.MethodDelete, "/" + TestBucket, ""},

		// Object operations (read)
		{"GetObject", http.MethodGet, "/" + TestBucket + "/some-key", ""},
		{"HeadObject", http.MethodHead, "/" + TestBucket + "/some-key", ""},

		// Object operations (write)
		{"PutObject", http.MethodPut, "/" + TestBucket + "/some-key", "content"},
		{"DeleteObject", http.MethodDelete, "/" + TestBucket + "/some-key", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var bodyReader io.Reader
			if tt.body != "" {
				bodyReader = strings.NewReader(tt.body)
			}

			req, err := http.NewRequest(tt.method, ts.URL()+tt.path, bodyReader)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusForbidden {
				t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
			}

			// HEAD responses have no body
			if tt.method == http.MethodHead {
				return
			}

			var s3Err s3.Error
			if err := xml.NewDecoder(resp.Body).Decode(&s3Err); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}

			if s3Err.Code != s3.ErrAccessDenied {
				t.Errorf("error code = %q, want %q", s3Err.Code, s3.ErrAccessDenied)
			}
		})
	}
}
