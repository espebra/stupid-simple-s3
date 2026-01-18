package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// Algorithm is the signing algorithm used
	Algorithm = "AWS4-HMAC-SHA256"
	// TimeFormat is the format used for X-Amz-Date
	TimeFormat = "20060102T150405Z"
	// DateFormat is the format used for the date in the credential scope
	DateFormat = "20060102"
	// MaxClockSkew is the maximum allowed time difference between request and server
	MaxClockSkew = 15 * time.Minute
	// MaxPresignedExpiry is the maximum allowed expiry time for presigned URLs (7 days)
	MaxPresignedExpiry = 7 * 24 * 60 * 60
)

// SignatureV4 handles AWS Signature Version 4 verification
type SignatureV4 struct{}

// AuthResult contains the result of signature verification
type AuthResult struct {
	AccessKeyID   string
	Region        string
	Service       string
	SignedHeaders []string
}

// ParsedAuthorization contains parsed authorization header components
type ParsedAuthorization struct {
	Algorithm     string
	AccessKeyID   string
	Date          string
	Region        string
	Service       string
	Request       string
	SignedHeaders []string
	Signature     string
}

// ParsedPresignedURL contains parsed presigned URL query parameters
type ParsedPresignedURL struct {
	Algorithm     string
	AccessKeyID   string
	Date          string
	Region        string
	Service       string
	Expires       int
	SignedHeaders []string
	Signature     string
	AmzDate       string
}

var authorizationRegex = regexp.MustCompile(
	`^AWS4-HMAC-SHA256\s+` +
		`Credential=([^/]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request,\s*` +
		`SignedHeaders=([^,]+),\s*` +
		`Signature=([a-f0-9]+)$`,
)

// ParseAuthorization parses the Authorization header
func (s *SignatureV4) ParseAuthorization(authHeader string) (*ParsedAuthorization, error) {
	matches := authorizationRegex.FindStringSubmatch(authHeader)
	if matches == nil {
		return nil, fmt.Errorf("invalid authorization header format")
	}

	return &ParsedAuthorization{
		Algorithm:     Algorithm,
		AccessKeyID:   matches[1],
		Date:          matches[2],
		Region:        matches[3],
		Service:       matches[4],
		Request:       "aws4_request",
		SignedHeaders: strings.Split(matches[5], ";"),
		Signature:     matches[6],
	}, nil
}

// VerifyRequest verifies the signature of an HTTP request
func (s *SignatureV4) VerifyRequest(r *http.Request, secretKey string) (*AuthResult, error) {
	// Parse authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, fmt.Errorf("missing Authorization header")
	}

	parsed, err := s.ParseAuthorization(authHeader)
	if err != nil {
		return nil, err
	}

	// Get and validate timestamp
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return nil, fmt.Errorf("missing X-Amz-Date header")
	}

	requestTime, err := time.Parse(TimeFormat, amzDate)
	if err != nil {
		return nil, fmt.Errorf("invalid X-Amz-Date format: %w", err)
	}

	// Check clock skew
	now := time.Now().UTC()
	diff := now.Sub(requestTime)
	if diff < 0 {
		diff = -diff
	}
	if diff > MaxClockSkew {
		return nil, fmt.Errorf("request time too skewed: %v", diff)
	}

	// Verify date in Authorization matches X-Amz-Date
	if parsed.Date != requestTime.Format(DateFormat) {
		return nil, fmt.Errorf("date mismatch between Authorization and X-Amz-Date")
	}

	// Build canonical request
	canonicalRequest := s.buildCanonicalRequest(r, parsed.SignedHeaders)

	// Build string to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsed.Date, parsed.Region, parsed.Service)
	stringToSign := s.buildStringToSign(amzDate, credentialScope, canonicalRequest)

	// Calculate expected signature
	signingKey := s.deriveSigningKey(secretKey, parsed.Date, parsed.Region, parsed.Service)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Compare signatures
	if !hmac.Equal([]byte(expectedSignature), []byte(parsed.Signature)) {
		return nil, fmt.Errorf("signature mismatch")
	}

	return &AuthResult{
		AccessKeyID:   parsed.AccessKeyID,
		Region:        parsed.Region,
		Service:       parsed.Service,
		SignedHeaders: parsed.SignedHeaders,
	}, nil
}

// buildCanonicalRequest creates the canonical request string
func (s *SignatureV4) buildCanonicalRequest(r *http.Request, signedHeaders []string) string {
	// HTTP method
	method := r.Method

	// Canonical URI (path)
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	// URI encode path components but preserve slashes
	canonicalURI = uriEncode(canonicalURI, false)

	// Canonical query string
	canonicalQueryString := s.buildCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := s.buildCanonicalHeaders(r, signedHeaders)

	// Signed headers (lowercase, sorted, semicolon-separated)
	signedHeadersStr := strings.Join(signedHeaders, ";")

	// Payload hash
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersStr,
		payloadHash,
	}, "\n")

	return canonicalRequest
}

// buildCanonicalQueryString creates the canonical query string
func (s *SignatureV4) buildCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	// Get all keys and sort them
	keys := make([]string, 0, len(query))
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build canonical query string
	var pairs []string
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, v := range values {
			pairs = append(pairs, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}

	return strings.Join(pairs, "&")
}

// buildCanonicalHeaders creates the canonical headers string
func (s *SignatureV4) buildCanonicalHeaders(r *http.Request, signedHeaders []string) string {
	var headers []string

	for _, h := range signedHeaders {
		var value string
		if h == "host" {
			value = r.Host
		} else {
			value = r.Header.Get(h)
		}
		// Trim whitespace and convert sequential spaces to single space
		value = strings.TrimSpace(value)
		headers = append(headers, strings.ToLower(h)+":"+value+"\n")
	}

	return strings.Join(headers, "")
}

// buildStringToSign creates the string to sign
func (s *SignatureV4) buildStringToSign(amzDate, credentialScope, canonicalRequest string) string {
	canonicalRequestHash := sha256.Sum256([]byte(canonicalRequest))

	return strings.Join([]string{
		Algorithm,
		amzDate,
		credentialScope,
		hex.EncodeToString(canonicalRequestHash[:]),
	}, "\n")
}

// deriveSigningKey derives the signing key using HMAC chain
func (s *SignatureV4) deriveSigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// hmacSHA256 calculates HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// uriEncode encodes a string for use in a URI
func uriEncode(s string, encodeSlash bool) string {
	var result strings.Builder
	for _, c := range []byte(s) {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-' || c == '~' || c == '.' {
			result.WriteByte(c)
		} else if c == '/' && !encodeSlash {
			result.WriteByte(c)
		} else {
			result.WriteString(fmt.Sprintf("%%%02X", c))
		}
	}
	return result.String()
}

// IsPresignedRequest checks if the request is a presigned URL request
func IsPresignedRequest(r *http.Request) bool {
	query := r.URL.Query()
	return query.Get("X-Amz-Algorithm") != "" &&
		query.Get("X-Amz-Credential") != "" &&
		query.Get("X-Amz-Signature") != ""
}

// ParsePresignedURL parses presigned URL query parameters
func (s *SignatureV4) ParsePresignedURL(r *http.Request) (*ParsedPresignedURL, error) {
	query := r.URL.Query()

	// Check algorithm
	algorithm := query.Get("X-Amz-Algorithm")
	if algorithm != Algorithm {
		return nil, fmt.Errorf("invalid algorithm: %s", algorithm)
	}

	// Parse credential: access-key/date/region/s3/aws4_request
	credential := query.Get("X-Amz-Credential")
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 || credParts[4] != "aws4_request" {
		return nil, fmt.Errorf("invalid credential format")
	}

	// Parse expires
	expiresStr := query.Get("X-Amz-Expires")
	expires, err := strconv.Atoi(expiresStr)
	if err != nil || expires <= 0 {
		return nil, fmt.Errorf("invalid expires value")
	}
	if expires > MaxPresignedExpiry {
		return nil, fmt.Errorf("expires exceeds maximum allowed value")
	}

	// Get date
	amzDate := query.Get("X-Amz-Date")
	if amzDate == "" {
		return nil, fmt.Errorf("missing X-Amz-Date")
	}

	// Get signed headers
	signedHeadersStr := query.Get("X-Amz-SignedHeaders")
	if signedHeadersStr == "" {
		return nil, fmt.Errorf("missing X-Amz-SignedHeaders")
	}
	signedHeaders := strings.Split(signedHeadersStr, ";")

	// Get signature
	signature := query.Get("X-Amz-Signature")
	if signature == "" {
		return nil, fmt.Errorf("missing X-Amz-Signature")
	}

	return &ParsedPresignedURL{
		Algorithm:     algorithm,
		AccessKeyID:   credParts[0],
		Date:          credParts[1],
		Region:        credParts[2],
		Service:       credParts[3],
		Expires:       expires,
		SignedHeaders: signedHeaders,
		Signature:     signature,
		AmzDate:       amzDate,
	}, nil
}

// VerifyPresignedRequest verifies the signature of a presigned URL request
func (s *SignatureV4) VerifyPresignedRequest(r *http.Request, secretKey string) (*AuthResult, error) {
	parsed, err := s.ParsePresignedURL(r)
	if err != nil {
		return nil, err
	}

	// Parse and validate timestamp
	requestTime, err := time.Parse(TimeFormat, parsed.AmzDate)
	if err != nil {
		return nil, fmt.Errorf("invalid X-Amz-Date format: %w", err)
	}

	// Verify date in credential matches X-Amz-Date
	if parsed.Date != requestTime.Format(DateFormat) {
		return nil, fmt.Errorf("date mismatch between credential and X-Amz-Date")
	}

	// Check if the presigned URL has expired
	now := time.Now().UTC()
	expiryTime := requestTime.Add(time.Duration(parsed.Expires) * time.Second)
	if now.After(expiryTime) {
		return nil, fmt.Errorf("presigned URL has expired")
	}

	// Check if the request time is in the future (with some tolerance)
	if requestTime.After(now.Add(MaxClockSkew)) {
		return nil, fmt.Errorf("request time is in the future")
	}

	// Build canonical request for presigned URL
	canonicalRequest := s.buildPresignedCanonicalRequest(r, parsed)

	// Build string to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsed.Date, parsed.Region, parsed.Service)
	stringToSign := s.buildStringToSign(parsed.AmzDate, credentialScope, canonicalRequest)

	// Calculate expected signature
	signingKey := s.deriveSigningKey(secretKey, parsed.Date, parsed.Region, parsed.Service)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Compare signatures
	if !hmac.Equal([]byte(expectedSignature), []byte(parsed.Signature)) {
		return nil, fmt.Errorf("signature mismatch")
	}

	return &AuthResult{
		AccessKeyID:   parsed.AccessKeyID,
		Region:        parsed.Region,
		Service:       parsed.Service,
		SignedHeaders: parsed.SignedHeaders,
	}, nil
}

// buildPresignedCanonicalRequest creates the canonical request for presigned URLs
func (s *SignatureV4) buildPresignedCanonicalRequest(r *http.Request, parsed *ParsedPresignedURL) string {
	// HTTP method
	method := r.Method

	// Canonical URI (path)
	canonicalURI := r.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalURI = uriEncode(canonicalURI, false)

	// Canonical query string (exclude X-Amz-Signature)
	canonicalQueryString := s.buildPresignedCanonicalQueryString(r.URL.Query())

	// Canonical headers
	canonicalHeaders := s.buildCanonicalHeaders(r, parsed.SignedHeaders)

	// Signed headers
	signedHeadersStr := strings.Join(parsed.SignedHeaders, ";")

	// For presigned URLs, the payload hash is always UNSIGNED-PAYLOAD
	payloadHash := "UNSIGNED-PAYLOAD"

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeadersStr,
		payloadHash,
	}, "\n")

	return canonicalRequest
}

// buildPresignedCanonicalQueryString creates the canonical query string for presigned URLs
// It excludes X-Amz-Signature from the canonical string
func (s *SignatureV4) buildPresignedCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	// Get all keys except X-Amz-Signature and sort them
	keys := make([]string, 0, len(query))
	for k := range query {
		if k != "X-Amz-Signature" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	// Build canonical query string
	var pairs []string
	for _, k := range keys {
		values := query[k]
		sort.Strings(values)
		for _, v := range values {
			pairs = append(pairs, uriEncode(k, true)+"="+uriEncode(v, true))
		}
	}

	return strings.Join(pairs, "&")
}

// GetPresignedAccessKeyID extracts the access key ID from a presigned URL request
func GetPresignedAccessKeyID(r *http.Request) string {
	credential := r.URL.Query().Get("X-Amz-Credential")
	if credential == "" {
		return ""
	}
	parts := strings.Split(credential, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
