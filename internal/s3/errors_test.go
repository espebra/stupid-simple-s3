package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewError(t *testing.T) {
	tests := []struct {
		code     ErrorCode
		resource string
		wantMsg  string
	}{
		{ErrAccessDenied, "/bucket/key", "Access Denied"},
		{ErrNoSuchKey, "/bucket/missing", "The specified key does not exist."},
		{ErrNoSuchBucket, "/wrong-bucket", "The specified bucket does not exist"},
		{ErrInternalError, "/bucket/key", "We encountered an internal error. Please try again."},
	}

	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			err := NewError(tt.code, tt.resource)

			if err.Code != tt.code {
				t.Errorf("Code = %q, want %q", err.Code, tt.code)
			}
			if err.Message != tt.wantMsg {
				t.Errorf("Message = %q, want %q", err.Message, tt.wantMsg)
			}
			if err.Resource != tt.resource {
				t.Errorf("Resource = %q, want %q", err.Resource, tt.resource)
			}
		})
	}
}

func TestErrorStatusCode(t *testing.T) {
	tests := []struct {
		code       ErrorCode
		wantStatus int
	}{
		{ErrAccessDenied, http.StatusForbidden},
		{ErrNoSuchKey, http.StatusNotFound},
		{ErrNoSuchBucket, http.StatusNotFound},
		{ErrInternalError, http.StatusInternalServerError},
		{ErrInvalidArgument, http.StatusBadRequest},
		{ErrMissingContentLength, http.StatusLengthRequired},
		{ErrMethodNotAllowed, http.StatusMethodNotAllowed},
		{ErrBucketNotEmpty, http.StatusConflict},
		{ErrSignatureDoesNotMatch, http.StatusForbidden},
		{ErrRequestTimeTooSkewed, http.StatusForbidden},
		{ErrInvalidAccessKeyId, http.StatusForbidden},
		{ErrExpiredToken, http.StatusForbidden},
	}

	for _, tt := range tests {
		t.Run(string(tt.code), func(t *testing.T) {
			err := NewError(tt.code, "/test")
			if got := err.StatusCode(); got != tt.wantStatus {
				t.Errorf("StatusCode() = %d, want %d", got, tt.wantStatus)
			}
		})
	}
}

func TestErrorStatusCodeUnknown(t *testing.T) {
	err := &Error{Code: "UnknownErrorCode"}
	if got := err.StatusCode(); got != http.StatusInternalServerError {
		t.Errorf("StatusCode() for unknown code = %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestErrorError(t *testing.T) {
	err := NewError(ErrAccessDenied, "/bucket/key")
	expected := "AccessDenied: Access Denied"
	if got := err.Error(); got != expected {
		t.Errorf("Error() = %q, want %q", got, expected)
	}
}

func TestErrorWriteResponse(t *testing.T) {
	err := NewError(ErrNoSuchKey, "/bucket/missing-key")

	w := httptest.NewRecorder()
	err.WriteResponse(w)

	// Check status code
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want %d", w.Code, http.StatusNotFound)
	}

	// Check content type
	if got := w.Header().Get("Content-Type"); got != "application/xml" {
		t.Errorf("Content-Type = %q, want %q", got, "application/xml")
	}

	// Check XML body
	var decoded Error
	if xmlErr := xml.NewDecoder(w.Body).Decode(&decoded); xmlErr != nil {
		t.Fatalf("failed to decode XML: %v", xmlErr)
	}

	if decoded.Code != ErrNoSuchKey {
		t.Errorf("decoded Code = %q, want %q", decoded.Code, ErrNoSuchKey)
	}
	if decoded.Resource != "/bucket/missing-key" {
		t.Errorf("decoded Resource = %q, want %q", decoded.Resource, "/bucket/missing-key")
	}
}

func TestAllErrorCodesHaveStatusCodes(t *testing.T) {
	codes := []ErrorCode{
		ErrAccessDenied,
		ErrBucketNotEmpty,
		ErrInternalError,
		ErrInvalidAccessKeyId,
		ErrInvalidArgument,
		ErrInvalidBucketName,
		ErrInvalidPart,
		ErrInvalidPartOrder,
		ErrInvalidRequest,
		ErrMalformedXML,
		ErrMethodNotAllowed,
		ErrMissingContentLength,
		ErrNoSuchBucket,
		ErrNoSuchKey,
		ErrNoSuchUpload,
		ErrRequestTimeTooSkewed,
		ErrSignatureDoesNotMatch,
		ErrEntityTooSmall,
		ErrIncompleteBody,
		ErrAuthorizationHeaderMalformed,
		ErrExpiredToken,
	}

	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			err := NewError(code, "/test")
			status := err.StatusCode()
			// Should not fall back to 500 for known error codes
			if status == http.StatusInternalServerError && code != ErrInternalError {
				t.Errorf("ErrorCode %q has no mapped status code", code)
			}
		})
	}
}

func TestAllErrorCodesHaveMessages(t *testing.T) {
	codes := []ErrorCode{
		ErrAccessDenied,
		ErrBucketNotEmpty,
		ErrInternalError,
		ErrInvalidAccessKeyId,
		ErrInvalidArgument,
		ErrInvalidBucketName,
		ErrInvalidPart,
		ErrInvalidPartOrder,
		ErrInvalidRequest,
		ErrMalformedXML,
		ErrMethodNotAllowed,
		ErrMissingContentLength,
		ErrNoSuchBucket,
		ErrNoSuchKey,
		ErrNoSuchUpload,
		ErrRequestTimeTooSkewed,
		ErrSignatureDoesNotMatch,
		ErrEntityTooSmall,
		ErrIncompleteBody,
		ErrAuthorizationHeaderMalformed,
		ErrExpiredToken,
	}

	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			err := NewError(code, "/test")
			if err.Message == "" {
				t.Errorf("ErrorCode %q has no message", code)
			}
		})
	}
}
