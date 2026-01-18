package auth

import (
	"net/http"
	"net/url"
	"testing"
)

// FuzzParseAuthorization tests the ParseAuthorization function with random inputs
// to find crashes or panics when parsing malformed authorization headers.
//
// Run with: go test -fuzz=FuzzParseAuthorization -fuzztime=30s ./internal/auth/
func FuzzParseAuthorization(f *testing.F) {
	// Add seed corpus with valid and edge-case inputs
	f.Add("AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024")
	f.Add("")
	f.Add("AWS4-HMAC-SHA256")
	f.Add("AWS4-HMAC-SHA256 Credential=")
	f.Add("AWS4-HMAC-SHA256 Credential=test/20130524/us-east-1/s3/aws4_request")
	f.Add("invalid header")
	f.Add("AWS4-HMAC-SHA256 Credential=key/date/region/service/aws4_request, SignedHeaders=, Signature=abc")

	sigv4 := &SignatureV4{}

	f.Fuzz(func(t *testing.T, authHeader string) {
		// This should never panic, regardless of input
		_, _ = sigv4.ParseAuthorization(authHeader)
	})
}

// FuzzParsePresignedURL tests the ParsePresignedURL function with random inputs
// to find crashes or panics when parsing malformed presigned URL parameters.
//
// Run with: go test -fuzz=FuzzParsePresignedURL -fuzztime=30s ./internal/auth/
func FuzzParsePresignedURL(f *testing.F) {
	// Add seed corpus
	f.Add("AWS4-HMAC-SHA256", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request", "86400", "20130524T000000Z", "host", "signature123")
	f.Add("", "", "", "", "", "")
	f.Add("AWS4-HMAC-SHA256", "invalid", "notanumber", "invalid-date", "", "")
	f.Add("INVALID-ALGORITHM", "key/date/region/service/aws4_request", "3600", "20130524T000000Z", "host", "sig")
	f.Add("AWS4-HMAC-SHA256", "a/b/c/d/e", "-1", "20130524T000000Z", "host;x-amz-date", "abc123")
	f.Add("AWS4-HMAC-SHA256", "key/20130524/region/s3/aws4_request", "999999999", "20130524T000000Z", "host", "sig")

	sigv4 := &SignatureV4{}

	f.Fuzz(func(t *testing.T, algorithm, credential, expires, amzDate, signedHeaders, signature string) {
		// Build a request with these query parameters
		queryParams := url.Values{}
		queryParams.Set("X-Amz-Algorithm", algorithm)
		queryParams.Set("X-Amz-Credential", credential)
		queryParams.Set("X-Amz-Expires", expires)
		queryParams.Set("X-Amz-Date", amzDate)
		queryParams.Set("X-Amz-SignedHeaders", signedHeaders)
		queryParams.Set("X-Amz-Signature", signature)

		req := &http.Request{
			Method: "GET",
			URL: &url.URL{
				Path:     "/bucket/key",
				RawQuery: queryParams.Encode(),
			},
			Host: "localhost",
		}

		// This should never panic, regardless of input
		_, _ = sigv4.ParsePresignedURL(req)
	})
}

// FuzzURIEncode tests the uriEncode function with random inputs
//
// Run with: go test -fuzz=FuzzURIEncode -fuzztime=30s ./internal/auth/
func FuzzURIEncode(f *testing.F) {
	// Add seed corpus
	f.Add("", true)
	f.Add("", false)
	f.Add("/path/to/file", true)
	f.Add("/path/to/file", false)
	f.Add("hello world", true)
	f.Add("special!@#$%^&*()chars", true)
	f.Add("unicode\u0000\u001f\u007f", true)
	f.Add(string([]byte{0x00, 0xff, 0x80}), true)

	f.Fuzz(func(t *testing.T, input string, encodeSlash bool) {
		// This should never panic, regardless of input
		result := uriEncode(input, encodeSlash)

		// Basic sanity check: result should not be empty if input has content
		// (except for truly empty input)
		if len(input) > 0 && len(result) == 0 {
			t.Errorf("uriEncode returned empty result for non-empty input")
		}
	})
}
