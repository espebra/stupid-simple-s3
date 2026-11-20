package auth

import (
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

const (
	rejTestAccessKey = "AKIAIOSFODNN7EXAMPLE"
	rejTestSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	rejTestZeroSig   = "0000000000000000000000000000000000000000000000000000000000000000"
)

// rejSignedRequest returns a GET request signed in the Authorization header
// with X-Amz-Date set to amzDate and the credential scope date set to
// scopeDate, so the two can differ.
func rejSignedRequest(t *testing.T, amzDate, scopeDate string) *http.Request {
	t.Helper()
	s := &SignatureV4{}
	req := httptest.NewRequest("GET", "/my-bucket/test.txt", nil)
	req.Host = "localhost:8080"
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	canonicalRequest, err := s.buildCanonicalRequest(req, signedHeaders)
	if err != nil {
		t.Fatal(err)
	}
	scope := scopeDate + "/us-east-1/s3/aws4_request"
	stringToSign := s.buildStringToSign(amzDate, scope, canonicalRequest)
	sig := hmacSHA256(s.deriveSigningKey(rejTestSecretKey, scopeDate, "us-east-1", "s3"), []byte(stringToSign))
	req.Header.Set("Authorization", Algorithm+" Credential="+rejTestAccessKey+"/"+scope+
		", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+hex.EncodeToString(sig))
	return req
}

// rejPresignedRequest returns a GET request for a URL presigned with the
// given X-Amz-Date, credential scope date and expiry. The signature is
// computed over these values, so only the checks under test can fail.
func rejPresignedRequest(t *testing.T, amzDate, scopeDate string, expires int) *http.Request {
	t.Helper()
	s := &SignatureV4{}
	query := "X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=" + rejTestAccessKey + "/" + scopeDate + "/us-east-1/s3/aws4_request" +
		"&X-Amz-Date=" + amzDate +
		"&X-Amz-Expires=" + strconv.Itoa(expires) +
		"&X-Amz-SignedHeaders=host"
	req := httptest.NewRequest("GET", "/my-bucket/test.txt?"+query, nil)
	req.Host = "localhost:8080"
	parsed := &ParsedPresignedURL{
		Date: scopeDate, Region: "us-east-1", Service: "s3",
		Expires: expires, SignedHeaders: []string{"host"}, AmzDate: amzDate,
	}
	canonicalRequest, err := s.buildPresignedCanonicalRequest(req, parsed)
	if err != nil {
		t.Fatal(err)
	}
	scope := scopeDate + "/us-east-1/s3/aws4_request"
	stringToSign := s.buildStringToSign(amzDate, scope, canonicalRequest)
	sig := hmacSHA256(s.deriveSigningKey(rejTestSecretKey, scopeDate, "us-east-1", "s3"), []byte(stringToSign))

	signed := httptest.NewRequest("GET", "/my-bucket/test.txt?"+query+"&X-Amz-Signature="+hex.EncodeToString(sig), nil)
	signed.Host = "localhost:8080"
	return signed
}

func TestVerifyRequestRejections(t *testing.T) {
	s := &SignatureV4{}
	now := time.Now().UTC()

	t.Run("sanity: correctly signed request verifies", func(t *testing.T) {
		req := rejSignedRequest(t, now.Format(TimeFormat), now.Format(DateFormat))
		if _, err := s.VerifyRequest(req, rejTestSecretKey); err != nil {
			t.Fatalf("VerifyRequest: %v", err)
		}
	})

	t.Run("date in the future beyond clock skew", func(t *testing.T) {
		future := now.Add(MaxClockSkew + 5*time.Minute)
		req := rejSignedRequest(t, future.Format(TimeFormat), future.Format(DateFormat))
		_, err := s.VerifyRequest(req, rejTestSecretKey)
		if !errors.Is(err, ErrRequestTimeTooSkewed) {
			t.Errorf("err = %v, want ErrRequestTimeTooSkewed", err)
		}
	})

	t.Run("date in the future within clock skew", func(t *testing.T) {
		future := now.Add(MaxClockSkew - time.Minute)
		req := rejSignedRequest(t, future.Format(TimeFormat), future.Format(DateFormat))
		if _, err := s.VerifyRequest(req, rejTestSecretKey); err != nil {
			t.Errorf("VerifyRequest: %v", err)
		}
	})

	t.Run("credential scope date differs from X-Amz-Date", func(t *testing.T) {
		otherDay := now.AddDate(0, 0, -1).Format(DateFormat)
		req := rejSignedRequest(t, now.Format(TimeFormat), otherDay)
		_, err := s.VerifyRequest(req, rejTestSecretKey)
		if !errors.Is(err, ErrDateMismatch) {
			t.Errorf("err = %v, want ErrDateMismatch", err)
		}
	})

	t.Run("malformed Authorization", func(t *testing.T) {
		req := rejSignedRequest(t, now.Format(TimeFormat), now.Format(DateFormat))
		req.Header.Set("Authorization", Algorithm+" Credential="+rejTestAccessKey+"/not-a-date/us-east-1/s3/aws4_request, SignedHeaders=host, Signature="+rejTestZeroSig)
		_, err := s.VerifyRequest(req, rejTestSecretKey)
		if !errors.Is(err, errMalformedAuthorization) {
			t.Errorf("err = %v, want errMalformedAuthorization", err)
		}
	})
}

func TestVerifyPresignedRequestRejections(t *testing.T) {
	s := &SignatureV4{}
	now := time.Now().UTC()

	t.Run("sanity: correctly presigned request verifies", func(t *testing.T) {
		req := rejPresignedRequest(t, now.Format(TimeFormat), now.Format(DateFormat), 3600)
		if _, err := s.VerifyPresignedRequest(req, rejTestSecretKey); err != nil {
			t.Fatalf("VerifyPresignedRequest: %v", err)
		}
	})

	t.Run("malformed X-Amz-Date", func(t *testing.T) {
		req := rejPresignedRequest(t, "2025-01-01T00:00:00Z", now.Format(DateFormat), 3600)
		_, err := s.VerifyPresignedRequest(req, rejTestSecretKey)
		if err == nil {
			t.Fatal("expected an error for a malformed X-Amz-Date")
		}
		if errors.Is(err, ErrSignatureMismatch) || errors.Is(err, ErrPresignedExpired) || errors.Is(err, ErrPresignedFuture) {
			t.Errorf("err = %v, want a date format error", err)
		}
	})

	t.Run("credential scope date differs from X-Amz-Date", func(t *testing.T) {
		otherDay := now.AddDate(0, 0, -1).Format(DateFormat)
		req := rejPresignedRequest(t, now.Format(TimeFormat), otherDay, 3600)
		_, err := s.VerifyPresignedRequest(req, rejTestSecretKey)
		if err == nil {
			t.Fatal("expected an error for mismatched dates")
		}
		if errors.Is(err, ErrSignatureMismatch) {
			t.Errorf("err = %v, want a date mismatch error", err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		past := now.Add(-2 * time.Hour)
		req := rejPresignedRequest(t, past.Format(TimeFormat), past.Format(DateFormat), 60)
		_, err := s.VerifyPresignedRequest(req, rejTestSecretKey)
		if !errors.Is(err, ErrPresignedExpired) {
			t.Errorf("err = %v, want ErrPresignedExpired", err)
		}
	})

	t.Run("in the future beyond clock skew", func(t *testing.T) {
		future := now.Add(MaxClockSkew + 5*time.Minute)
		req := rejPresignedRequest(t, future.Format(TimeFormat), future.Format(DateFormat), 3600)
		_, err := s.VerifyPresignedRequest(req, rejTestSecretKey)
		if !errors.Is(err, ErrPresignedFuture) {
			t.Errorf("err = %v, want ErrPresignedFuture", err)
		}
	})

	t.Run("valid URL signed with another secret", func(t *testing.T) {
		req := rejPresignedRequest(t, now.Format(TimeFormat), now.Format(DateFormat), 3600)
		_, err := s.VerifyPresignedRequest(req, "a-different-secret-key")
		if !errors.Is(err, ErrSignatureMismatch) {
			t.Errorf("err = %v, want ErrSignatureMismatch", err)
		}
	})

	t.Run("valid URL with tampered expiry", func(t *testing.T) {
		req := rejPresignedRequest(t, now.Format(TimeFormat), now.Format(DateFormat), 3600)
		q := req.URL.Query()
		q.Set("X-Amz-Expires", "7200")
		tampered := httptest.NewRequest("GET", "/my-bucket/test.txt?"+q.Encode(), nil)
		tampered.Host = req.Host
		_, err := s.VerifyPresignedRequest(tampered, rejTestSecretKey)
		if !errors.Is(err, ErrSignatureMismatch) {
			t.Errorf("err = %v, want ErrSignatureMismatch", err)
		}
	})
}
