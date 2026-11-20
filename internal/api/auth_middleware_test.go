package api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	dto "github.com/prometheus/client_model/go"

	"github.com/espen/stupid-simple-s3/internal/auth"
	"github.com/espen/stupid-simple-s3/internal/config"
	"github.com/espen/stupid-simple-s3/internal/metrics"
	"github.com/espen/stupid-simple-s3/internal/s3"
)

const (
	authMWAccessKey = "AKIAAUTHMWTEST"
	authMWSecretKey = "authMiddlewareTestSecretKey/EXAMPLE"
	authMWURL       = "http://localhost:8080/test-bucket/some/key.txt"
	authMWRegion    = "us-east-1"
)

func authMWConfig() *config.Config {
	return &config.Config{
		Credentials: []config.Credential{
			{AccessKeyID: authMWAccessKey, SecretAccessKey: authMWSecretKey, Privileges: config.PrivilegeReadWrite},
		},
	}
}

// authMWSignedRequest returns a GET request signed in the Authorization
// header with the AWS SDK's own signer, so the server is checked against an
// independent implementation.
func authMWSignedRequest(t *testing.T, accessKey, secretKey string, signingTime time.Time) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, authMWURL, nil)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	if err := v4.NewSigner().SignHTTP(context.Background(), creds, req, "UNSIGNED-PAYLOAD", "s3", authMWRegion, signingTime); err != nil {
		t.Fatalf("SignHTTP: %v", err)
	}
	return req
}

// authMWPresignedRequest returns a GET request for a URL presigned with the
// AWS SDK's signer at signingTime, valid for expires.
func authMWPresignedRequest(t *testing.T, accessKey, secretKey string, signingTime time.Time, expires time.Duration) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, authMWURL, nil)
	q := req.URL.Query()
	q.Set("X-Amz-Expires", strconv.Itoa(int(expires/time.Second)))
	req.URL.RawQuery = q.Encode()
	creds := aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey}
	signed, _, err := v4.NewSigner().PresignHTTP(context.Background(), creds, req, "UNSIGNED-PAYLOAD", "s3", authMWRegion, signingTime)
	if err != nil {
		t.Fatalf("PresignHTTP: %v", err)
	}
	return httptest.NewRequest(http.MethodGet, signed, nil)
}

// authMWWithQueryParam returns a copy of req with the query parameter key set to value
func authMWWithQueryParam(req *http.Request, key, value string) *http.Request {
	u := *req.URL
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return httptest.NewRequest(req.Method, u.String(), nil)
}

func authMWFailureCount(t *testing.T, reason string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.AuthFailuresTotal.WithLabelValues(reason).Write(&m); err != nil {
		t.Fatalf("read auth failure counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

type authMWCase struct {
	name       string
	req        func(t *testing.T) *http.Request
	wantStatus int
	wantCode   s3.ErrorCode // empty means the request must be accepted
	wantReason string       // metrics.AuthReason* label incremented on rejection
	wantMsg    string       // the error message, if set
}

func runAuthMWCases(t *testing.T, cases []authMWCase) {
	t.Helper()
	mw := AuthMiddleware(authMWConfig())

	reasons := map[string]float64{}
	for _, tc := range cases {
		if tc.wantReason != "" {
			reasons[tc.wantReason]++
		}
	}
	before := map[string]float64{}
	for r := range reasons {
		before[r] = authMWFailureCount(t, r)
	}

	// Rejections sleep authFailureDelay (a const), so run in parallel
	t.Run("cases", func(t *testing.T) {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				var called atomic.Bool
				var gotCred *config.Credential
				h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					called.Store(true)
					gotCred = GetCredential(r)
					w.WriteHeader(http.StatusOK)
				}))

				w := httptest.NewRecorder()
				h.ServeHTTP(w, tc.req(t))

				if tc.wantCode == "" {
					if !called.Load() {
						t.Fatalf("next handler not called; status %d, body %s", w.Code, w.Body.String())
					}
					if w.Code != http.StatusOK {
						t.Errorf("status = %d, want 200", w.Code)
					}
					if gotCred == nil || gotCred.AccessKeyID != authMWAccessKey {
						t.Errorf("credential in context = %+v, want access key %q", gotCred, authMWAccessKey)
					}
					return
				}

				if called.Load() {
					t.Error("next handler was called for a rejected request")
				}
				if w.Code != tc.wantStatus {
					t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
				}
				var body s3.Error
				if err := xml.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatalf("parse error body %q: %v", w.Body.String(), err)
				}
				if body.Code != tc.wantCode {
					t.Errorf("error code = %q, want %q", body.Code, tc.wantCode)
				}
				if tc.wantMsg != "" && body.Message != tc.wantMsg {
					t.Errorf("error message = %q, want %q", body.Message, tc.wantMsg)
				}
			})
		}
	})

	// At least the expected increments: other tests could run concurrently
	for r, n := range reasons {
		if got := authMWFailureCount(t, r) - before[r]; got < n {
			t.Errorf("auth_failures_total{reason=%q} increased by %v, want %v", r, got, n)
		}
	}
}

func TestAuthMiddlewareHeaderAuth(t *testing.T) {
	now := time.Now().UTC()

	runAuthMWCases(t, []authMWCase{
		{
			name: "missing Authorization",
			req: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodGet, authMWURL, nil)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrAccessDenied,
			wantReason: metrics.AuthReasonMissingHeader,
		},
		{
			name: "malformed Authorization",
			req: func(t *testing.T) *http.Request {
				req := authMWSignedRequest(t, authMWAccessKey, authMWSecretKey, now)
				req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=garbage")
				return req
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationHeaderMalformed,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "unknown access key",
			req: func(t *testing.T) *http.Request {
				return authMWSignedRequest(t, "AKIAUNKNOWN", authMWSecretKey, now)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrInvalidAccessKeyId,
			wantReason: metrics.AuthReasonInvalidAccessKey,
		},
		{
			name: "date too far in the past",
			req: func(t *testing.T) *http.Request {
				return authMWSignedRequest(t, authMWAccessKey, authMWSecretKey, now.Add(-auth.MaxClockSkew-5*time.Minute))
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrRequestTimeTooSkewed,
			wantReason: metrics.AuthReasonTimeSkew,
		},
		{
			name: "date too far in the future",
			req: func(t *testing.T) *http.Request {
				return authMWSignedRequest(t, authMWAccessKey, authMWSecretKey, now.Add(auth.MaxClockSkew+5*time.Minute))
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrRequestTimeTooSkewed,
			wantReason: metrics.AuthReasonTimeSkew,
		},
		{
			name: "bad signature",
			req: func(t *testing.T) *http.Request {
				return authMWSignedRequest(t, authMWAccessKey, "wrong-secret", now)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrSignatureDoesNotMatch,
			wantReason: metrics.AuthReasonSignatureMismatch,
		},
		{
			name: "valid",
			req: func(t *testing.T) *http.Request {
				return authMWSignedRequest(t, authMWAccessKey, authMWSecretKey, now)
			},
		},
	})
}

func TestAuthMiddlewarePresignedAuth(t *testing.T) {
	now := time.Now().UTC()

	runAuthMWCases(t, []authMWCase{
		{
			name: "malformed credential with empty access key",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				cred := "/" + now.Format(auth.DateFormat) + "/us-east-1/s3/aws4_request"
				return authMWWithQueryParam(req, "X-Amz-Credential", cred)
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "credential without scope",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				return authMWWithQueryParam(req, "X-Amz-Credential", authMWAccessKey)
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			// The query parameters are checked before the access key
			name: "credential without scope and unknown access key",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				return authMWWithQueryParam(req, "X-Amz-Credential", "AKIAUNKNOWN")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "expires over a week",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				return authMWWithQueryParam(req, "X-Amz-Expires", "604801")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "missing X-Amz-Date",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				return authMWWithQueryParam(req, "X-Amz-Date", "")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "malformed X-Amz-Date",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				return authMWWithQueryParam(req, "X-Amz-Date", "yesterday")
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   s3.ErrAuthorizationQueryParametersError,
			wantReason: metrics.AuthReasonMalformedHeader,
		},
		{
			name: "unknown access key",
			req: func(t *testing.T) *http.Request {
				return authMWPresignedRequest(t, "AKIAUNKNOWN", authMWSecretKey, now, time.Hour)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrInvalidAccessKeyId,
			wantReason: metrics.AuthReasonInvalidAccessKey,
		},
		{
			name: "expired",
			req: func(t *testing.T) *http.Request {
				return authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now.Add(-2*time.Hour), time.Minute)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrAccessDenied,
			wantMsg:    "Request has expired",
			wantReason: metrics.AuthReasonTimeSkew,
		},
		{
			name: "date in the future beyond clock skew",
			req: func(t *testing.T) *http.Request {
				return authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now.Add(auth.MaxClockSkew+5*time.Minute), time.Hour)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrAccessDenied,
			wantMsg:    "Request is not valid yet",
			wantReason: metrics.AuthReasonTimeSkew,
		},
		{
			name: "bad signature",
			req: func(t *testing.T) *http.Request {
				return authMWPresignedRequest(t, authMWAccessKey, "wrong-secret", now, time.Hour)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrSignatureDoesNotMatch,
			wantReason: metrics.AuthReasonSignatureMismatch,
		},
		{
			name: "tampered path",
			req: func(t *testing.T) *http.Request {
				req := authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
				u, err := url.Parse(req.URL.String())
				if err != nil {
					t.Fatal(err)
				}
				u.Path = "/test-bucket/other-key.txt"
				return httptest.NewRequest(http.MethodGet, u.String(), nil)
			},
			wantStatus: http.StatusForbidden,
			wantCode:   s3.ErrSignatureDoesNotMatch,
			wantReason: metrics.AuthReasonSignatureMismatch,
		},
		{
			name: "valid",
			req: func(t *testing.T) *http.Request {
				return authMWPresignedRequest(t, authMWAccessKey, authMWSecretKey, now, time.Hour)
			},
		},
	})
}
