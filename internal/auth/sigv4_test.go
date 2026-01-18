package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseAuthorization(t *testing.T) {
	sigv4 := &SignatureV4{}

	tests := []struct {
		name        string
		authHeader  string
		wantErr     bool
		wantKeyID   string
		wantRegion  string
		wantService string
		wantHeaders []string
	}{
		{
			name: "valid authorization header",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, " +
				"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
				"Signature=abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
			wantErr:     false,
			wantKeyID:   "AKIAIOSFODNN7EXAMPLE",
			wantRegion:  "us-east-1",
			wantService: "s3",
			wantHeaders: []string{"host", "x-amz-content-sha256", "x-amz-date"},
		},
		{
			name:       "empty header",
			authHeader: "",
			wantErr:    true,
		},
		{
			name:       "invalid algorithm",
			authHeader: "AWS4-HMAC-SHA512 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123",
			wantErr:    true,
		},
		{
			name:       "malformed header",
			authHeader: "AWS4-HMAC-SHA256 garbage",
			wantErr:    true,
		},
		{
			name:       "missing signature",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, SignedHeaders=host",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := sigv4.ParseAuthorization(tt.authHeader)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if parsed.AccessKeyID != tt.wantKeyID {
				t.Errorf("AccessKeyID = %q, want %q", parsed.AccessKeyID, tt.wantKeyID)
			}
			if parsed.Region != tt.wantRegion {
				t.Errorf("Region = %q, want %q", parsed.Region, tt.wantRegion)
			}
			if parsed.Service != tt.wantService {
				t.Errorf("Service = %q, want %q", parsed.Service, tt.wantService)
			}
			if len(parsed.SignedHeaders) != len(tt.wantHeaders) {
				t.Errorf("SignedHeaders = %v, want %v", parsed.SignedHeaders, tt.wantHeaders)
			}
		})
	}
}

func TestDeriveSigningKey(t *testing.T) {
	sigv4 := &SignatureV4{}

	// Test vector from AWS documentation
	secretKey := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	date := "20150830"
	region := "us-east-1"
	service := "iam"

	key := sigv4.deriveSigningKey(secretKey, date, region, service)

	// The derived key should be 32 bytes (SHA256 output)
	if len(key) != 32 {
		t.Errorf("signing key length = %d, want 32", len(key))
	}

	// Verify it's not empty/zero
	allZero := true
	for _, b := range key {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("signing key is all zeros")
	}
}

func TestBuildCanonicalQueryString(t *testing.T) {
	sigv4 := &SignatureV4{}

	tests := []struct {
		name   string
		query  map[string][]string
		expect string
	}{
		{
			name:   "empty query",
			query:  map[string][]string{},
			expect: "",
		},
		{
			name:   "single param",
			query:  map[string][]string{"foo": {"bar"}},
			expect: "foo=bar",
		},
		{
			name:   "multiple params sorted",
			query:  map[string][]string{"z": {"last"}, "a": {"first"}},
			expect: "a=first&z=last",
		},
		{
			name:   "param with special chars",
			query:  map[string][]string{"key": {"value with spaces"}},
			expect: "key=value%20with%20spaces",
		},
		{
			name:   "multiple values same key",
			query:  map[string][]string{"key": {"b", "a"}},
			expect: "key=a&key=b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sigv4.buildCanonicalQueryString(tt.query)
			if result != tt.expect {
				t.Errorf("got %q, want %q", result, tt.expect)
			}
		})
	}
}

func TestURIEncode(t *testing.T) {
	tests := []struct {
		input       string
		encodeSlash bool
		expect      string
	}{
		{"hello", false, "hello"},
		{"hello world", false, "hello%20world"},
		{"path/to/file", false, "path/to/file"},
		{"path/to/file", true, "path%2Fto%2Ffile"},
		{"a-b_c.d~e", false, "a-b_c.d~e"},
		{"special!@#$", false, "special%21%40%23%24"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := uriEncode(tt.input, tt.encodeSlash)
			if result != tt.expect {
				t.Errorf("uriEncode(%q, %v) = %q, want %q", tt.input, tt.encodeSlash, result, tt.expect)
			}
		})
	}
}

func TestVerifyRequest(t *testing.T) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	// Create a request with a valid time
	now := time.Now().UTC()
	amzDate := now.Format(TimeFormat)
	dateStamp := now.Format(DateFormat)

	t.Run("missing authorization header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt", nil)
		_, err := sigv4.VerifyRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for missing Authorization header")
		}
	})

	t.Run("missing x-amz-date header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt", nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"+dateStamp+"/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123")
		_, err := sigv4.VerifyRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for missing X-Amz-Date header")
		}
	})

	t.Run("time skew too large", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt", nil)
		oldTime := time.Now().UTC().Add(-20 * time.Minute)
		oldAmzDate := oldTime.Format(TimeFormat)
		oldDateStamp := oldTime.Format(DateFormat)

		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"+oldDateStamp+"/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123")
		req.Header.Set("X-Amz-Date", oldAmzDate)

		_, err := sigv4.VerifyRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for time skew")
		}
		if !strings.Contains(err.Error(), "skewed") {
			t.Errorf("expected time skew error, got: %v", err)
		}
	})

	t.Run("signature mismatch", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt", nil)
		req.Host = "localhost:8080"
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/"+dateStamp+"/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=0000000000000000000000000000000000000000000000000000000000000000")
		req.Header.Set("X-Amz-Date", amzDate)
		req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

		_, err := sigv4.VerifyRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for signature mismatch")
		}
		if !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("expected signature mismatch error, got: %v", err)
		}
	})
}

func TestBuildCanonicalHeaders(t *testing.T) {
	sigv4 := &SignatureV4{}

	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Host = "example.com"
	req.Header.Set("X-Amz-Date", "20230101T000000Z")
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	result := sigv4.buildCanonicalHeaders(req, signedHeaders)

	expected := "host:example.com\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:20230101T000000Z\n"
	if result != expected {
		t.Errorf("canonical headers:\ngot:  %q\nwant: %q", result, expected)
	}
}

func TestBuildStringToSign(t *testing.T) {
	sigv4 := &SignatureV4{}

	amzDate := "20230101T120000Z"
	credentialScope := "20230101/us-east-1/s3/aws4_request"
	canonicalRequest := "GET\n/\n\nhost:example.com\n\nhost\nUNSIGNED-PAYLOAD"

	result := sigv4.buildStringToSign(amzDate, credentialScope, canonicalRequest)

	if !strings.HasPrefix(result, "AWS4-HMAC-SHA256\n") {
		t.Error("string to sign should start with algorithm")
	}
	if !strings.Contains(result, amzDate) {
		t.Error("string to sign should contain amz date")
	}
	if !strings.Contains(result, credentialScope) {
		t.Error("string to sign should contain credential scope")
	}
}

// Helper to create a properly signed request for integration testing
func createSignedRequest(method, path string, secretKey, accessKeyID string) *http.Request {
	sigv4 := &SignatureV4{}

	now := time.Now().UTC()
	amzDate := now.Format(TimeFormat)
	dateStamp := now.Format(DateFormat)

	req := httptest.NewRequest(method, path, nil)
	req.Host = "localhost:8080"
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}

	canonicalRequest := sigv4.buildCanonicalRequest(req, signedHeaders)
	credentialScope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := sigv4.buildStringToSign(amzDate, credentialScope, canonicalRequest)
	signingKey := sigv4.deriveSigningKey(secretKey, dateStamp, "us-east-1", "s3")

	signature := hmacSHA256(signingKey, []byte(stringToSign))

	authHeader := "AWS4-HMAC-SHA256 Credential=" + accessKeyID + "/" + credentialScope +
		", SignedHeaders=" + strings.Join(signedHeaders, ";") +
		", Signature=" + string(toHex(signature))

	req.Header.Set("Authorization", authHeader)

	return req
}

func toHex(data []byte) []byte {
	const hexChars = "0123456789abcdef"
	result := make([]byte, len(data)*2)
	for i, b := range data {
		result[i*2] = hexChars[b>>4]
		result[i*2+1] = hexChars[b&0x0f]
	}
	return result
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func TestSignedRequestVerification(t *testing.T) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	accessKeyID := "AKIAIOSFODNN7EXAMPLE"

	req := createSignedRequest("GET", "/my-bucket/test.txt", secretKey, accessKeyID)

	result, err := sigv4.VerifyRequest(req, secretKey)
	if err != nil {
		t.Fatalf("failed to verify signed request: %v", err)
	}

	if result.AccessKeyID != accessKeyID {
		t.Errorf("AccessKeyID = %q, want %q", result.AccessKeyID, accessKeyID)
	}
}

func TestIsPresignedRequest(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect bool
	}{
		{
			name:   "presigned request",
			query:  "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Signature=abc123",
			expect: true,
		},
		{
			name:   "regular request",
			query:  "",
			expect: false,
		},
		{
			name:   "partial presigned params",
			query:  "?X-Amz-Algorithm=AWS4-HMAC-SHA256",
			expect: false,
		},
		{
			name:   "regular query params",
			query:  "?list-type=2&prefix=test",
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/my-bucket/test.txt"+tt.query, nil)
			result := IsPresignedRequest(req)
			if result != tt.expect {
				t.Errorf("IsPresignedRequest() = %v, want %v", result, tt.expect)
			}
		})
	}
}

func TestParsePresignedURL(t *testing.T) {
	sigv4 := &SignatureV4{}

	t.Run("valid presigned URL", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Date=20230101T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		parsed, err := sigv4.ParsePresignedURL(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if parsed.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
			t.Errorf("AccessKeyID = %q, want AKIAIOSFODNN7EXAMPLE", parsed.AccessKeyID)
		}
		if parsed.Date != "20230101" {
			t.Errorf("Date = %q, want 20230101", parsed.Date)
		}
		if parsed.Region != "us-east-1" {
			t.Errorf("Region = %q, want us-east-1", parsed.Region)
		}
		if parsed.Expires != 3600 {
			t.Errorf("Expires = %d, want 3600", parsed.Expires)
		}
	})

	t.Run("invalid algorithm", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA512&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Date=20230101T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		_, err := sigv4.ParsePresignedURL(req)
		if err == nil {
			t.Error("expected error for invalid algorithm")
		}
	})

	t.Run("invalid credential format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101&X-Amz-Date=20230101T120000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		_, err := sigv4.ParsePresignedURL(req)
		if err == nil {
			t.Error("expected error for invalid credential format")
		}
	})

	t.Run("invalid expires", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Date=20230101T120000Z&X-Amz-Expires=invalid&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		_, err := sigv4.ParsePresignedURL(req)
		if err == nil {
			t.Error("expected error for invalid expires")
		}
	})

	t.Run("expires too large", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Date=20230101T120000Z&X-Amz-Expires=999999999&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		_, err := sigv4.ParsePresignedURL(req)
		if err == nil {
			t.Error("expected error for expires too large")
		}
	})

	t.Run("missing X-Amz-Date", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/my-bucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=abc123", nil)
		_, err := sigv4.ParsePresignedURL(req)
		if err == nil {
			t.Error("expected error for missing X-Amz-Date")
		}
	})
}

func TestGetPresignedAccessKeyID(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		expect string
	}{
		{
			name:   "valid credential",
			query:  "?X-Amz-Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request",
			expect: "AKIAIOSFODNN7EXAMPLE",
		},
		{
			name:   "no credential",
			query:  "",
			expect: "",
		},
		{
			name:   "empty credential",
			query:  "?X-Amz-Credential=",
			expect: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/my-bucket/test.txt"+tt.query, nil)
			result := GetPresignedAccessKeyID(req)
			if result != tt.expect {
				t.Errorf("GetPresignedAccessKeyID() = %q, want %q", result, tt.expect)
			}
		})
	}
}

// Helper to create a properly signed presigned URL request
func createPresignedRequest(method, path string, secretKey, accessKeyID string, expires int) *http.Request {
	sigv4 := &SignatureV4{}

	now := time.Now().UTC()
	amzDate := now.Format(TimeFormat)
	dateStamp := now.Format(DateFormat)

	// Build the presigned URL query parameters
	credential := accessKeyID + "/" + dateStamp + "/us-east-1/s3/aws4_request"
	signedHeaders := "host"

	// Create request with query parameters (without signature first)
	queryParams := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=" + credential +
		"&X-Amz-Date=" + amzDate +
		"&X-Amz-Expires=" + itoa(expires) +
		"&X-Amz-SignedHeaders=" + signedHeaders

	req := httptest.NewRequest(method, path+"?"+queryParams, nil)
	req.Host = "localhost:8080"

	// Build canonical request for presigned URL
	parsed := &ParsedPresignedURL{
		Algorithm:     Algorithm,
		AccessKeyID:   accessKeyID,
		Date:          dateStamp,
		Region:        "us-east-1",
		Service:       "s3",
		Expires:       expires,
		SignedHeaders: []string{"host"},
		AmzDate:       amzDate,
	}

	canonicalRequest := sigv4.buildPresignedCanonicalRequest(req, parsed)
	credentialScope := dateStamp + "/us-east-1/s3/aws4_request"
	stringToSign := sigv4.buildStringToSign(amzDate, credentialScope, canonicalRequest)
	signingKey := sigv4.deriveSigningKey(secretKey, dateStamp, "us-east-1", "s3")

	signature := hmacSHA256(signingKey, []byte(stringToSign))
	signatureHex := string(toHex(signature))

	// Create the final request with signature
	finalQuery := queryParams + "&X-Amz-Signature=" + signatureHex
	return httptest.NewRequest(method, path+"?"+finalQuery, nil)
}

func TestVerifyPresignedRequest(t *testing.T) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	accessKeyID := "AKIAIOSFODNN7EXAMPLE"

	t.Run("valid presigned request", func(t *testing.T) {
		req := createPresignedRequest("GET", "/my-bucket/test.txt", secretKey, accessKeyID, 3600)
		req.Host = "localhost:8080"

		result, err := sigv4.VerifyPresignedRequest(req, secretKey)
		if err != nil {
			t.Fatalf("failed to verify presigned request: %v", err)
		}

		if result.AccessKeyID != accessKeyID {
			t.Errorf("AccessKeyID = %q, want %q", result.AccessKeyID, accessKeyID)
		}
	})

	t.Run("invalid signature", func(t *testing.T) {
		now := time.Now().UTC()
		amzDate := now.Format(TimeFormat)
		dateStamp := now.Format(DateFormat)

		query := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=" + accessKeyID + "/" + dateStamp + "/us-east-1/s3/aws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=3600" +
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=0000000000000000000000000000000000000000000000000000000000000000"

		req := httptest.NewRequest("GET", "/my-bucket/test.txt?"+query, nil)
		req.Host = "localhost:8080"

		_, err := sigv4.VerifyPresignedRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for invalid signature")
		}
		if !strings.Contains(err.Error(), "mismatch") {
			t.Errorf("expected signature mismatch error, got: %v", err)
		}
	})

	t.Run("expired presigned URL", func(t *testing.T) {
		// Create a request with a date in the past
		oldTime := time.Now().UTC().Add(-2 * time.Hour)
		amzDate := oldTime.Format(TimeFormat)
		dateStamp := oldTime.Format(DateFormat)

		query := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=" + accessKeyID + "/" + dateStamp + "/us-east-1/s3/aws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=60" + // 1 minute expiry
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=abc123"

		req := httptest.NewRequest("GET", "/my-bucket/test.txt?"+query, nil)
		req.Host = "localhost:8080"

		_, err := sigv4.VerifyPresignedRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for expired presigned URL")
		}
		if !strings.Contains(err.Error(), "expired") {
			t.Errorf("expected expired error, got: %v", err)
		}
	})

	t.Run("future request time", func(t *testing.T) {
		// Create a request with a date far in the future
		futureTime := time.Now().UTC().Add(30 * time.Minute)
		amzDate := futureTime.Format(TimeFormat)
		dateStamp := futureTime.Format(DateFormat)

		query := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=" + accessKeyID + "/" + dateStamp + "/us-east-1/s3/aws4_request" +
			"&X-Amz-Date=" + amzDate +
			"&X-Amz-Expires=3600" +
			"&X-Amz-SignedHeaders=host" +
			"&X-Amz-Signature=abc123"

		req := httptest.NewRequest("GET", "/my-bucket/test.txt?"+query, nil)
		req.Host = "localhost:8080"

		_, err := sigv4.VerifyPresignedRequest(req, secretKey)
		if err == nil {
			t.Error("expected error for future request time")
		}
		if !strings.Contains(err.Error(), "future") {
			t.Errorf("expected future time error, got: %v", err)
		}
	})
}

func TestBuildPresignedCanonicalQueryString(t *testing.T) {
	sigv4 := &SignatureV4{}

	tests := []struct {
		name   string
		query  map[string][]string
		expect string
	}{
		{
			name: "excludes X-Amz-Signature",
			query: map[string][]string{
				"X-Amz-Algorithm":     {"AWS4-HMAC-SHA256"},
				"X-Amz-Credential":    {"AKID/20230101/us-east-1/s3/aws4_request"},
				"X-Amz-Signature":     {"shouldbeexcluded"},
				"X-Amz-SignedHeaders": {"host"},
			},
			expect: "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKID%2F20230101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-SignedHeaders=host",
		},
		{
			name:   "empty query",
			query:  map[string][]string{},
			expect: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sigv4.buildPresignedCanonicalQueryString(tt.query)
			if result != tt.expect {
				t.Errorf("got %q, want %q", result, tt.expect)
			}
		})
	}
}
