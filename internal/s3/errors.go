package s3

import (
	"encoding/xml"
	"net/http"
)

type ErrorCode string

const (
	ErrAccessDenied                      ErrorCode = "AccessDenied"
	ErrBadDigest                         ErrorCode = "BadDigest"
	ErrBucketAlreadyOwnedByYou           ErrorCode = "BucketAlreadyOwnedByYou"
	ErrBucketNotEmpty                    ErrorCode = "BucketNotEmpty"
	ErrInternalError                     ErrorCode = "InternalError"
	ErrInvalidAccessKeyId                ErrorCode = "InvalidAccessKeyId"
	ErrInvalidArgument                   ErrorCode = "InvalidArgument"
	ErrInvalidBucketName                 ErrorCode = "InvalidBucketName"
	ErrInvalidPart                       ErrorCode = "InvalidPart"
	ErrInvalidPartOrder                  ErrorCode = "InvalidPartOrder"
	ErrInvalidRequest                    ErrorCode = "InvalidRequest"
	ErrMalformedXML                      ErrorCode = "MalformedXML"
	ErrMethodNotAllowed                  ErrorCode = "MethodNotAllowed"
	ErrMissingContentLength              ErrorCode = "MissingContentLength"
	ErrNoSuchBucket                      ErrorCode = "NoSuchBucket"
	ErrNoSuchKey                         ErrorCode = "NoSuchKey"
	ErrNoSuchUpload                      ErrorCode = "NoSuchUpload"
	ErrRequestTimeTooSkewed              ErrorCode = "RequestTimeTooSkewed"
	ErrSignatureDoesNotMatch             ErrorCode = "SignatureDoesNotMatch"
	ErrEntityTooSmall                    ErrorCode = "EntityTooSmall"
	ErrIncompleteBody                    ErrorCode = "IncompleteBody"
	ErrAuthorizationHeaderMalformed      ErrorCode = "AuthorizationHeaderMalformed"
	ErrAuthorizationQueryParametersError ErrorCode = "AuthorizationQueryParametersError"
	ErrEntityTooLarge                    ErrorCode = "EntityTooLarge"
	ErrInvalidRange                      ErrorCode = "InvalidRange"
	ErrXAmzContentSHA256Mismatch         ErrorCode = "XAmzContentSHA256Mismatch"
	ErrPreconditionFailed                ErrorCode = "PreconditionFailed"
	ErrNotImplemented                    ErrorCode = "NotImplemented"
)

var errorStatusCodes = map[ErrorCode]int{
	ErrAccessDenied:                      http.StatusForbidden,
	ErrBadDigest:                         http.StatusBadRequest,
	ErrBucketAlreadyOwnedByYou:           http.StatusConflict,
	ErrBucketNotEmpty:                    http.StatusConflict,
	ErrInternalError:                     http.StatusInternalServerError,
	ErrInvalidAccessKeyId:                http.StatusForbidden,
	ErrInvalidArgument:                   http.StatusBadRequest,
	ErrInvalidBucketName:                 http.StatusBadRequest,
	ErrInvalidPart:                       http.StatusBadRequest,
	ErrInvalidPartOrder:                  http.StatusBadRequest,
	ErrInvalidRequest:                    http.StatusBadRequest,
	ErrMalformedXML:                      http.StatusBadRequest,
	ErrMethodNotAllowed:                  http.StatusMethodNotAllowed,
	ErrMissingContentLength:              http.StatusLengthRequired,
	ErrNoSuchBucket:                      http.StatusNotFound,
	ErrNoSuchKey:                         http.StatusNotFound,
	ErrNoSuchUpload:                      http.StatusNotFound,
	ErrRequestTimeTooSkewed:              http.StatusForbidden,
	ErrSignatureDoesNotMatch:             http.StatusForbidden,
	ErrXAmzContentSHA256Mismatch:         http.StatusBadRequest,
	ErrPreconditionFailed:                http.StatusPreconditionFailed,
	ErrNotImplemented:                    http.StatusNotImplemented,
	ErrEntityTooSmall:                    http.StatusBadRequest,
	ErrIncompleteBody:                    http.StatusBadRequest,
	ErrAuthorizationHeaderMalformed:      http.StatusBadRequest,
	ErrAuthorizationQueryParametersError: http.StatusBadRequest,
	ErrEntityTooLarge:                    http.StatusRequestEntityTooLarge,
	ErrInvalidRange:                      http.StatusRequestedRangeNotSatisfiable,
}

var errorMessages = map[ErrorCode]string{
	ErrAccessDenied:                      "Access Denied",
	ErrBadDigest:                         "The SHA256 you specified did not match the calculated checksum.",
	ErrBucketAlreadyOwnedByYou:           "Your previous request to create the named bucket succeeded and you already own it.",
	ErrBucketNotEmpty:                    "The bucket you tried to delete is not empty",
	ErrInternalError:                     "We encountered an internal error. Please try again.",
	ErrInvalidAccessKeyId:                "The AWS Access Key Id you provided does not exist in our records.",
	ErrInvalidArgument:                   "Invalid Argument",
	ErrInvalidBucketName:                 "The specified bucket is not valid.",
	ErrInvalidPart:                       "One or more of the specified parts could not be found.",
	ErrInvalidPartOrder:                  "The list of parts was not in ascending order.",
	ErrInvalidRequest:                    "Invalid Request",
	ErrMalformedXML:                      "The XML you provided was not well-formed or did not validate against our published schema.",
	ErrMethodNotAllowed:                  "The specified method is not allowed against this resource.",
	ErrMissingContentLength:              "You must provide the Content-Length HTTP header.",
	ErrNoSuchBucket:                      "The specified bucket does not exist",
	ErrNoSuchKey:                         "The specified key does not exist.",
	ErrNoSuchUpload:                      "The specified multipart upload does not exist.",
	ErrRequestTimeTooSkewed:              "The difference between the request time and the server's time is too large.",
	ErrSignatureDoesNotMatch:             "The request signature we calculated does not match the signature you provided.",
	ErrXAmzContentSHA256Mismatch:         "The provided 'x-amz-content-sha256' header does not match what was computed.",
	ErrPreconditionFailed:                "At least one of the pre-conditions you specified did not hold",
	ErrNotImplemented:                    "A header you provided implies functionality that is not implemented",
	ErrEntityTooSmall:                    "Your proposed upload is smaller than the minimum allowed object size.",
	ErrIncompleteBody:                    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
	ErrAuthorizationHeaderMalformed:      "The authorization header is malformed.",
	ErrAuthorizationQueryParametersError: "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
	ErrEntityTooLarge:                    "Your proposed upload exceeds the maximum allowed object size.",
	ErrInvalidRange:                      "The requested range is not valid.",
}

type Error struct {
	XMLName   xml.Name  `xml:"Error"`
	Code      ErrorCode `xml:"Code"`
	Message   string    `xml:"Message"`
	Resource  string    `xml:"Resource,omitempty"`
	RequestID string    `xml:"RequestId,omitempty"`
}

func NewError(code ErrorCode, resource string) *Error {
	return &Error{
		Code:     code,
		Message:  errorMessages[code],
		Resource: resource,
	}
}

func (e *Error) StatusCode() int {
	if code, ok := errorStatusCodes[e.Code]; ok {
		return code
	}
	return http.StatusInternalServerError
}

func (e *Error) Error() string {
	return string(e.Code) + ": " + e.Message
}

func (e *Error) WriteResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.StatusCode())
	_ = xml.NewEncoder(w).Encode(e)
}

// WriteErrorResponse writes an error response without including the resource path.
// This prevents information disclosure in error messages.
func WriteErrorResponse(w http.ResponseWriter, code ErrorCode) {
	WriteErrorResponseMessage(w, code, errorMessages[code])
}

// WriteErrorResponseMessage writes an error response with a message other
// than the code's default, for codes that S3 uses for several errors, such as
// AccessDenied for an expired presigned URL. Like WriteErrorResponse, it
// omits the resource path.
func WriteErrorResponseMessage(w http.ResponseWriter, code ErrorCode, message string) {
	err := &Error{
		Code:    code,
		Message: message,
		// Intentionally omit Resource to prevent information disclosure
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(err.StatusCode())
	_ = xml.NewEncoder(w).Encode(err)
}
