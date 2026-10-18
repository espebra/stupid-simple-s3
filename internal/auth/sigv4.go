package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
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

// Errors returned by VerifyRequest. They are fixed values that never include
// request data, so they are safe to log.
var (
	ErrMissingAuthorization = errors.New("missing Authorization header")
	ErrMissingDate          = errors.New("missing X-Amz-Date header")
	ErrInvalidDate          = errors.New("invalid X-Amz-Date format")
	ErrRequestTimeTooSkewed = errors.New("request time too skewed")
	ErrDateMismatch         = errors.New("date mismatch between Authorization and X-Amz-Date")
	ErrInvalidIPHeader      = errors.New("invalid IP address in a signed IP header")
	ErrSignatureMismatch    = errors.New("signature mismatch")
)

// SignatureV4 handles AWS Signature Version 4 verification. The zero value
// is ready to use, and it is safe for concurrent use.
type SignatureV4 struct {
	// signingKeys caches derived signing keys. A signing key depends only
	// on the secret key, date, region and service, so it changes once a
	// day per credential, while deriving it takes four HMAC-SHA256
	// computations. Only the keys of requests whose signature verified are
	// added, so clients without a valid secret key cannot fill it. The
	// cached slices are shared and must not be modified.
	signingKeysMu sync.RWMutex
	signingKeys   map[signingKeyID][]byte
}

// signingKeyID identifies a derived signing key
type signingKeyID struct {
	secretKey, date, region, service string
}

// maxCachedSigningKeys bounds SignatureV4.signingKeys. Only a few keys are
// in use at a time, one per credential, region and service for the current
// day (and the previous one around midnight), so the cache is simply
// cleared when it is full.
const maxCachedSigningKeys = 64

// signingKey returns the signing key for id, and whether it came from the
// cache. A key that did not should be passed to cacheSigningKey once a
// signature made with it has verified.
func (s *SignatureV4) signingKey(id signingKeyID) (key []byte, cached bool) {
	s.signingKeysMu.RLock()
	key, cached = s.signingKeys[id]
	s.signingKeysMu.RUnlock()
	if cached {
		return key, true
	}
	return s.deriveSigningKey(id.secretKey, id.date, id.region, id.service), false
}

// cacheSigningKey adds a signing key that verified a signature to the cache
func (s *SignatureV4) cacheSigningKey(id signingKeyID, key []byte) {
	s.signingKeysMu.Lock()
	defer s.signingKeysMu.Unlock()
	if s.signingKeys == nil || len(s.signingKeys) >= maxCachedSigningKeys {
		s.signingKeys = make(map[signingKeyID][]byte)
	}
	s.signingKeys[id] = key
}

// AuthResult contains the result of signature verification
type AuthResult struct {
	AccessKeyID   string
	Region        string
	Service       string
	SignedHeaders []string
	// ChunkVerifier verifies the chunk signatures of a signed aws-chunked
	// upload. It is nil for other requests.
	ChunkVerifier *ChunkVerifier
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

// errMalformedAuthorization is returned for an Authorization header that
// does not have the form
// "AWS4-HMAC-SHA256 Credential=<key>/<yyyymmdd>/<region>/<service>/aws4_request, SignedHeaders=<headers>, Signature=<hex>"
var errMalformedAuthorization = errors.New("invalid authorization header format")

// ParseAuthorization parses the Authorization header. It is called for
// every signed request, so it is written out by hand rather than with a
// regular expression, which took about 15% of the CPU time of a HEAD
// request. It accepts exactly what the regular expression
//
//	^AWS4-HMAC-SHA256\s+Credential=([^/]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request,\s*SignedHeaders=([^,]+),\s*Signature=([a-f0-9]+)$
//
// matches, which FuzzParseAuthorization checks.
func (s *SignatureV4) ParseAuthorization(authHeader string) (*ParsedAuthorization, error) {
	afterAlgorithm, ok := strings.CutPrefix(authHeader, Algorithm)
	if !ok {
		return nil, errMalformedAuthorization
	}
	// At least one space between the algorithm and the credential
	rest := trimLeftRegexpSpace(afterAlgorithm)
	if len(rest) == len(afterAlgorithm) {
		return nil, errMalformedAuthorization
	}
	if rest, ok = strings.CutPrefix(rest, "Credential="); !ok {
		return nil, errMalformedAuthorization
	}

	var p ParsedAuthorization
	var date string
	if p.AccessKeyID, rest, ok = strings.Cut(rest, "/"); !ok || p.AccessKeyID == "" {
		return nil, errMalformedAuthorization
	}
	if date, rest, ok = strings.Cut(rest, "/"); !ok || !isDigits(date, 8) {
		return nil, errMalformedAuthorization
	}
	if p.Region, rest, ok = strings.Cut(rest, "/"); !ok || p.Region == "" {
		return nil, errMalformedAuthorization
	}
	if p.Service, rest, ok = strings.Cut(rest, "/"); !ok || p.Service == "" {
		return nil, errMalformedAuthorization
	}
	if rest, ok = strings.CutPrefix(rest, "aws4_request,"); !ok {
		return nil, errMalformedAuthorization
	}
	if rest, ok = strings.CutPrefix(trimLeftRegexpSpace(rest), "SignedHeaders="); !ok {
		return nil, errMalformedAuthorization
	}
	var signedHeaders string
	if signedHeaders, rest, ok = strings.Cut(rest, ","); !ok || signedHeaders == "" {
		return nil, errMalformedAuthorization
	}
	if p.Signature, ok = strings.CutPrefix(trimLeftRegexpSpace(rest), "Signature="); !ok || !isLowerHex(p.Signature) {
		return nil, errMalformedAuthorization
	}

	p.Algorithm = Algorithm
	p.Date = date
	p.Request = "aws4_request"
	p.SignedHeaders = strings.Split(signedHeaders, ";")
	return &p, nil
}

// trimLeftRegexpSpace removes the leading characters that \s matches in a
// regular expression: tab, newline, form feed, carriage return and space
func trimLeftRegexpSpace(s string) string {
	return strings.TrimLeft(s, "\t\n\f\r ")
}

// isDigits reports whether s is n ASCII digits
func isDigits(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// isLowerHex reports whether s is one or more lowercase hex digits
func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if (s[i] < '0' || s[i] > '9') && (s[i] < 'a' || s[i] > 'f') {
			return false
		}
	}
	return true
}

// VerifyRequest verifies the signature of an HTTP request
func (s *SignatureV4) VerifyRequest(r *http.Request, secretKey string) (*AuthResult, error) {
	// Parse authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, ErrMissingAuthorization
	}

	parsed, err := s.ParseAuthorization(authHeader)
	if err != nil {
		return nil, err
	}

	// Get and validate timestamp
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		return nil, ErrMissingDate
	}

	requestTime, err := time.Parse(TimeFormat, amzDate)
	if err != nil {
		return nil, ErrInvalidDate
	}

	// Check clock skew
	now := time.Now().UTC()
	diff := now.Sub(requestTime)
	if diff < 0 {
		diff = -diff
	}
	if diff > MaxClockSkew {
		return nil, ErrRequestTimeTooSkewed
	}

	// Verify date in Authorization matches X-Amz-Date
	if parsed.Date != requestTime.Format(DateFormat) {
		return nil, ErrDateMismatch
	}

	// Build canonical request
	canonicalRequest, err := s.buildCanonicalRequest(r, parsed.SignedHeaders)
	if err != nil {
		return nil, err
	}

	// Build string to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsed.Date, parsed.Region, parsed.Service)
	stringToSign := s.buildStringToSign(amzDate, credentialScope, canonicalRequest)

	// Calculate expected signature
	keyID := signingKeyID{secretKey, parsed.Date, parsed.Region, parsed.Service}
	signingKey, cached := s.signingKey(keyID)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Compare signatures
	if !hmac.Equal([]byte(expectedSignature), []byte(parsed.Signature)) {
		return nil, ErrSignatureMismatch
	}
	if !cached {
		s.cacheSigningKey(keyID, signingKey)
	}

	result := &AuthResult{
		AccessKeyID:   parsed.AccessKeyID,
		Region:        parsed.Region,
		Service:       parsed.Service,
		SignedHeaders: parsed.SignedHeaders,
	}
	if payloadHash := r.Header.Get("X-Amz-Content-Sha256"); payloadHash == StreamingPayload || payloadHash == StreamingPayloadTrailer {
		result.ChunkVerifier = &ChunkVerifier{
			signingKey:    signingKey,
			amzDate:       amzDate,
			scope:         credentialScope,
			prevSignature: parsed.Signature,
		}
	}
	return result, nil
}

// buildCanonicalRequest creates the canonical request string
func (s *SignatureV4) buildCanonicalRequest(r *http.Request, signedHeaders []string) (string, error) {
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
	canonicalHeaders, err := s.buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", err
	}

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

	return canonicalRequest, nil
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

// ipHeaders lists headers that should contain valid IP addresses
var ipHeaders = map[string]bool{
	"client-ip":        true,
	"x-client-ip":      true,
	"x-forwarded-for":  true,
	"x-real-ip":        true,
	"true-client-ip":   true,
	"cf-connecting-ip": true,
}

// isValidIPHeader checks if a header value contains a valid IP address (or list of IPs)
func isValidIPHeader(value string) bool {
	if value == "" {
		return true // Empty is acceptable
	}

	// X-Forwarded-For can contain multiple IPs separated by commas
	parts := strings.Split(value, ",")
	for _, part := range parts {
		ip := strings.TrimSpace(part)
		if ip == "" {
			continue
		}
		// Check if it's a valid IPv4 or IPv6 address
		if net.ParseIP(ip) == nil {
			return false
		}
	}
	return true
}

// buildCanonicalHeaders creates the canonical headers string
func (s *SignatureV4) buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	var headers strings.Builder

	for _, h := range signedHeaders {
		var value string
		lowerH := strings.ToLower(h)
		if lowerH == "host" {
			value = canonicalHeaderValue([]string{r.Host})
		} else {
			value = canonicalHeaderValue(r.Header.Values(h))
		}

		// Validate IP-related headers contain valid IP addresses
		if ipHeaders[lowerH] && !isValidIPHeader(value) {
			return "", ErrInvalidIPHeader
		}

		headers.WriteString(lowerH)
		headers.WriteByte(':')
		headers.WriteString(value)
		headers.WriteByte('\n')
	}

	return headers.String(), nil
}

// canonicalHeaderValue returns the canonical form of a signed header's
// values: each trimmed, with runs of spaces inside it collapsed to one
// space, and joined with commas when the header was sent more than once.
// This is how the AWS SDKs sign them, so a value such as an x-amz-meta-*
// header with two spaces in a row only verifies when it is canonicalized
// the same way. values is not modified; it can be the request's own.
func canonicalHeaderValue(values []string) string {
	if len(values) == 1 && !strings.Contains(values[0], "  ") {
		return strings.TrimSpace(values[0])
	}
	canonical := make([]string, len(values))
	for i, v := range values {
		v = strings.Join(strings.FieldsFunc(v, func(r rune) bool { return r == ' ' }), " ")
		canonical[i] = strings.TrimSpace(v)
	}
	return strings.Join(canonical, ",")
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
			fmt.Fprintf(&result, "%%%02X", c)
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
	// Validate no empty parts in credential
	for i, part := range credParts {
		if part == "" {
			return nil, fmt.Errorf("invalid credential format: empty component at position %d", i)
		}
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
	// Validate signature is 64-character hex string (SHA256-HMAC = 256 bits = 64 hex chars)
	if len(signature) != 64 {
		return nil, fmt.Errorf("invalid X-Amz-Signature length: expected 64, got %d", len(signature))
	}
	for _, c := range signature {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return nil, fmt.Errorf("invalid X-Amz-Signature: not a valid hex string")
		}
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
	canonicalRequest, err := s.buildPresignedCanonicalRequest(r, parsed)
	if err != nil {
		return nil, err
	}

	// Build string to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", parsed.Date, parsed.Region, parsed.Service)
	stringToSign := s.buildStringToSign(parsed.AmzDate, credentialScope, canonicalRequest)

	// Calculate expected signature
	keyID := signingKeyID{secretKey, parsed.Date, parsed.Region, parsed.Service}
	signingKey, cached := s.signingKey(keyID)
	expectedSignature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Compare signatures
	if !hmac.Equal([]byte(expectedSignature), []byte(parsed.Signature)) {
		return nil, ErrSignatureMismatch
	}
	if !cached {
		s.cacheSigningKey(keyID, signingKey)
	}

	return &AuthResult{
		AccessKeyID:   parsed.AccessKeyID,
		Region:        parsed.Region,
		Service:       parsed.Service,
		SignedHeaders: parsed.SignedHeaders,
	}, nil
}

// buildPresignedCanonicalRequest creates the canonical request for presigned URLs
func (s *SignatureV4) buildPresignedCanonicalRequest(r *http.Request, parsed *ParsedPresignedURL) (string, error) {
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
	canonicalHeaders, err := s.buildCanonicalHeaders(r, parsed.SignedHeaders)
	if err != nil {
		return "", err
	}

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

	return canonicalRequest, nil
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
