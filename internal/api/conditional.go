package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/espen/stupid-simple-s3/internal/s3"
)

// checkPreconditions evaluates the conditional request headers of a GET or
// HEAD against the object, in the order of RFC 9110 section 13.2.2, which
// is also how S3 evaluates them. It returns 0 if the request should be
// served, http.StatusPreconditionFailed if If-Match or If-Unmodified-Since
// fails, or http.StatusNotModified if If-None-Match matches or the object
// has not been modified since If-Modified-Since.
func checkPreconditions(r *http.Request, meta *s3.ObjectMetadata) int {
	// Last-Modified is sent with second precision, so compare at that precision
	lastModified := meta.LastModified.UTC().Truncate(time.Second)

	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if !etagListMatches(ifMatch, meta.ETag, true) {
			return http.StatusPreconditionFailed
		}
	} else if t, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); err == nil {
		if lastModified.After(t) {
			return http.StatusPreconditionFailed
		}
	}

	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if etagListMatches(ifNoneMatch, meta.ETag, false) {
			return http.StatusNotModified
		}
	} else if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil {
		if !lastModified.After(t) {
			return http.StatusNotModified
		}
	}

	return 0
}

// etagListMatches reports whether a comma-separated If-Match or
// If-None-Match header value matches etag. "*" matches any existing object.
// Weak entity tags (W/"...") only match with weak comparison, which
// If-None-Match uses and If-Match does not.
func etagListMatches(header, etag string, strong bool) bool {
	if strings.TrimSpace(header) == "*" {
		return true
	}
	etag = strings.Trim(etag, `"`)
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if weak, ok := strings.CutPrefix(candidate, "W/"); ok {
			if strong {
				continue
			}
			candidate = weak
		}
		if strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}

// writePreconditionResponse writes the response for a failed precondition
// returned by checkPreconditions. A 304 response carries the validators of
// the object and no body.
func writePreconditionResponse(w http.ResponseWriter, status int, meta *s3.ObjectMetadata) {
	if status == http.StatusNotModified {
		w.Header().Set("ETag", meta.ETag)
		w.Header().Set("Last-Modified", meta.LastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusNotModified)
		return
	}
	s3.WriteErrorResponse(w, s3.ErrPreconditionFailed)
}
