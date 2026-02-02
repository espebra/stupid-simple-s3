package auth

import (
	"net/http"
	"net/url"
	"strings"
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

// FuzzIsValidIPHeader tests the isValidIPHeader function with random inputs
// to find crashes or incorrect validation of IP address headers.
//
// Run with: go test -fuzz=FuzzIsValidIPHeader -fuzztime=30s ./internal/auth/
func FuzzIsValidIPHeader(f *testing.F) {
	// Add seed corpus with valid and edge-case inputs
	f.Add("")
	f.Add("192.168.1.1")
	f.Add("10.0.0.1, 192.168.1.1")
	f.Add("::1")
	f.Add("2001:db8::1")
	f.Add("192.168.1.1, 10.0.0.1, 172.16.0.1")
	f.Add("invalid")
	f.Add("192.168.1.256")
	f.Add("192.168.1.1,")
	f.Add(", 192.168.1.1")
	f.Add("  192.168.1.1  ")
	f.Add("192.168.1.1, invalid, 10.0.0.1")
	f.Add("::ffff:192.168.1.1")
	f.Add("fe80::1%eth0")

	f.Fuzz(func(t *testing.T, input string) {
		// This should never panic, regardless of input
		_ = isValidIPHeader(input)
	})
}

// FuzzBuildCanonicalQueryStringSingle tests the buildCanonicalQueryString function
// with a single random query parameter to find crashes or panics.
//
// Run with: go test -fuzz=FuzzBuildCanonicalQueryStringSingle -fuzztime=30s ./internal/auth/
func FuzzBuildCanonicalQueryStringSingle(f *testing.F) {
	// Add seed corpus
	f.Add("", "")
	f.Add("key", "value")
	f.Add("a", "1")
	f.Add("key with spaces", "value with spaces")
	f.Add("special!@#$", "chars%^&*()")
	f.Add("unicode", "日本語")
	f.Add("empty", "")
	f.Add("", "empty-key")
	f.Add("duplicate", "value1")
	f.Add("z-key", "first")

	sigv4 := &SignatureV4{}

	f.Fuzz(func(t *testing.T, key, value string) {
		// Build query values with the fuzzed inputs
		query := url.Values{}
		query.Set(key, value)

		// This should never panic, regardless of input
		_ = sigv4.buildCanonicalQueryString(query)
	})
}

// FuzzBuildCanonicalQueryStringMultiple tests buildCanonicalQueryString
// with multiple query parameters.
//
// Run with: go test -fuzz=FuzzBuildCanonicalQueryStringMultiple -fuzztime=30s ./internal/auth/
func FuzzBuildCanonicalQueryStringMultiple(f *testing.F) {
	// Add seed corpus with multiple key-value pairs
	f.Add("a", "1", "b", "2", "c", "3")
	f.Add("z", "last", "a", "first", "m", "middle")
	f.Add("key", "val1", "key", "val2", "key", "val3")
	f.Add("", "", "", "", "", "")
	f.Add("X-Amz-Algorithm", "AWS4-HMAC-SHA256", "X-Amz-Credential", "test", "X-Amz-Date", "20130524T000000Z")

	sigv4 := &SignatureV4{}

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3 string) {
		query := url.Values{}
		query.Add(k1, v1)
		query.Add(k2, v2)
		query.Add(k3, v3)

		// This should never panic, regardless of input
		_ = sigv4.buildCanonicalQueryString(query)
	})
}

// FuzzGetPresignedAccessKeyID tests the GetPresignedAccessKeyID function
// with random credential values.
//
// Run with: go test -fuzz=FuzzGetPresignedAccessKeyID -fuzztime=30s ./internal/auth/
func FuzzGetPresignedAccessKeyID(f *testing.F) {
	// Add seed corpus
	f.Add("")
	f.Add("AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request")
	f.Add("key/date/region/service/aws4_request")
	f.Add("noslashes")
	f.Add("/leading/slash")
	f.Add("trailing/slash/")
	f.Add("////")
	f.Add("a/b/c/d/e/f/g")
	f.Add("special!@#$/date/region/service/aws4_request")

	f.Fuzz(func(t *testing.T, credential string) {
		req := &http.Request{
			URL: &url.URL{
				Path:     "/bucket/key",
				RawQuery: "X-Amz-Credential=" + url.QueryEscape(credential),
			},
		}

		// This should never panic, regardless of input
		result := GetPresignedAccessKeyID(req)

		// If credential has slashes, result should be the first part
		if credential != "" && strings.Contains(credential, "/") {
			parts := strings.Split(credential, "/")
			if result != parts[0] {
				t.Errorf("GetPresignedAccessKeyID returned %q, expected %q", result, parts[0])
			}
		}
	})
}

// FuzzBuildCanonicalHeaders tests the buildCanonicalHeaders function
// with random header values.
//
// Run with: go test -fuzz=FuzzBuildCanonicalHeaders -fuzztime=30s ./internal/auth/
func FuzzBuildCanonicalHeaders(f *testing.F) {
	// Add seed corpus
	f.Add("localhost", "20130524T000000Z", "content-type", "application/json")
	f.Add("example.com:8080", "20130524T000000Z", "x-amz-date", "20130524T000000Z")
	f.Add("", "", "", "")
	f.Add("host with spaces", "invalid date", "x-forwarded-for", "192.168.1.1")
	f.Add("localhost", "date", "x-forwarded-for", "invalid-ip")
	f.Add("localhost", "date", "x-real-ip", "10.0.0.1, 192.168.1.1")

	sigv4 := &SignatureV4{}

	f.Fuzz(func(t *testing.T, host, amzDate, headerName, headerValue string) {
		req := &http.Request{
			Host:   host,
			Header: http.Header{},
			URL:    &url.URL{Path: "/"},
		}
		if headerName != "" {
			req.Header.Set(headerName, headerValue)
		}
		req.Header.Set("X-Amz-Date", amzDate)

		signedHeaders := []string{"host"}
		if headerName != "" {
			signedHeaders = append(signedHeaders, strings.ToLower(headerName))
		}

		// This should never panic, regardless of input
		// It may return an error for invalid IP headers, which is expected
		_, _ = sigv4.buildCanonicalHeaders(req, signedHeaders)
	})
}
