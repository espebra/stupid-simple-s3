package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func BenchmarkParseAuthorization(b *testing.B) {
	sigv4 := &SignatureV4{}
	authHeader := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20230101/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date, " +
		"Signature=abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := sigv4.ParseAuthorization(authHeader)
		if err != nil {
			b.Fatalf("ParseAuthorization failed: %v", err)
		}
	}
}

func BenchmarkDeriveSigningKey(b *testing.B) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	date := "20230101"
	region := "us-east-1"
	service := "s3"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sigv4.deriveSigningKey(secretKey, date, region, service)
	}
}

func BenchmarkBuildCanonicalRequest(b *testing.B) {
	sigv4 := &SignatureV4{}

	req := httptest.NewRequest("PUT", "/my-bucket/path/to/large/object/key.txt?partNumber=1&uploadId=abc123", nil)
	req.Host = "localhost:8080"
	req.Header.Set("X-Amz-Date", "20230101T120000Z")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", "1048576")

	signedHeaders := []string{"content-length", "content-type", "host", "x-amz-content-sha256", "x-amz-date"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sigv4.buildCanonicalRequest(req, signedHeaders)
	}
}

func BenchmarkBuildStringToSign(b *testing.B) {
	sigv4 := &SignatureV4{}
	amzDate := "20230101T120000Z"
	credentialScope := "20230101/us-east-1/s3/aws4_request"
	canonicalRequest := "PUT\n/my-bucket/test.txt\n\nhost:localhost:8080\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:20230101T120000Z\n\nhost;x-amz-content-sha256;x-amz-date\nUNSIGNED-PAYLOAD"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = sigv4.buildStringToSign(amzDate, credentialScope, canonicalRequest)
	}
}

func BenchmarkBuildCanonicalQueryString(b *testing.B) {
	sigv4 := &SignatureV4{}

	queries := []map[string][]string{
		{}, // empty
		{"partNumber": {"1"}, "uploadId": {"abc123"}},
		{"a": {"1"}, "b": {"2"}, "c": {"3"}, "d": {"4"}, "e": {"5"}},
		{"prefix": {"path/to/objects/"}, "delimiter": {"/"}, "max-keys": {"1000"}, "marker": {"last-key"}},
	}

	for i, query := range queries {
		name := []string{"empty", "two_params", "five_params", "list_params"}[i]
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = sigv4.buildCanonicalQueryString(query)
			}
		})
	}
}

func BenchmarkURIEncode(b *testing.B) {
	tests := []struct {
		name        string
		input       string
		encodeSlash bool
	}{
		{"simple", "simple-key", false},
		{"with_slash", "path/to/object", false},
		{"with_slash_encoded", "path/to/object", true},
		{"special_chars", "file with spaces & special=chars!", false},
		{"long_path", strings.Repeat("segment/", 20) + "file.txt", false},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = uriEncode(tt.input, tt.encodeSlash)
			}
		})
	}
}

func BenchmarkVerifyRequest(b *testing.B) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	accessKeyID := "AKIAIOSFODNN7EXAMPLE"

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create a fresh signed request for each iteration
		req := createSignedRequestForBench(secretKey, accessKeyID)
		b.StartTimer()

		_, _ = sigv4.VerifyRequest(req, secretKey)
	}
}

func createSignedRequestForBench(secretKey, accessKeyID string) *http.Request {
	sigv4 := &SignatureV4{}

	now := time.Now().UTC()
	amzDate := now.Format(TimeFormat)
	dateStamp := now.Format(DateFormat)

	req := httptest.NewRequest("GET", "/my-bucket/test-object.txt", nil)
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
		", Signature=" + hexEncode(signature)

	req.Header.Set("Authorization", authHeader)

	return req
}

func hexEncode(data []byte) string {
	const hexChars = "0123456789abcdef"
	result := make([]byte, len(data)*2)
	for i, b := range data {
		result[i*2] = hexChars[b>>4]
		result[i*2+1] = hexChars[b&0x0f]
	}
	return string(result)
}

func BenchmarkFullSignatureFlow(b *testing.B) {
	sigv4 := &SignatureV4{}
	secretKey := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

	now := time.Now().UTC()
	amzDate := now.Format(TimeFormat)
	dateStamp := now.Format(DateFormat)

	req := httptest.NewRequest("PUT", "/my-bucket/objects/test-file.txt", nil)
	req.Host = "s3.example.com:8080"
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("Content-Type", "application/octet-stream")

	signedHeaders := []string{"content-type", "host", "x-amz-content-sha256", "x-amz-date"}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Canonical request
		canonicalRequest := sigv4.buildCanonicalRequest(req, signedHeaders)

		// Credential scope and string to sign
		credentialScope := dateStamp + "/us-east-1/s3/aws4_request"
		stringToSign := sigv4.buildStringToSign(amzDate, credentialScope, canonicalRequest)

		// Signing key and signature
		signingKey := sigv4.deriveSigningKey(secretKey, dateStamp, "us-east-1", "s3")
		_ = hmacSHA256(signingKey, []byte(stringToSign))
	}
}
