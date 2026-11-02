package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// scratchBucket is the bucket a worker creates and deletes again
func (s *soak) scratchBucket(id int) string {
	suffix := fmt.Sprintf("-w%03d", id)
	return s.cfg.bucket[:min(len(s.cfg.bucket), 63-len(suffix))] + suffix
}

// scratchKey is the object put in a scratch bucket
const scratchKey = "scratch"

// bucketOps checks the bucket operations: HEAD and location of the soak
// bucket, a bucket that does not exist, an invalid bucket name, and the
// life of a scratch bucket of the worker's own
func (w *worker) bucketOps(ctx context.Context) {
	bucket := &w.s.cfg.bucket
	missing := w.s.scratchBucket(w.id) + "-missing"
	switch w.rng.IntN(4) {
	case 0:
		if _, err := w.s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); err != nil {
			w.fail("bucket", *bucket, "head: %v", err)
		}
		_, err := w.s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &missing})
		if err := expectError(err, http.StatusNotFound, ""); err != nil {
			w.fail("bucket", missing, "head of a missing bucket: %v", err)
		}
		_, err = w.s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &missing, Key: aws.String(scratchKey)})
		if err := expectError(err, http.StatusNotFound, "NoSuchBucket"); err != nil {
			w.fail("bucket", missing, "get from a missing bucket: %v", err)
		}
	case 1:
		out, err := w.s.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: bucket})
		if err != nil {
			w.fail("bucket", *bucket, "location: %v", err)
		} else if out.LocationConstraint != "" {
			w.fail("bucket", *bucket, "location %q, want none", out.LocationConstraint)
		}
	case 2:
		invalid := "Invalid_Bucket"
		_, err := w.s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &invalid})
		if err := expectError(err, http.StatusBadRequest, "InvalidBucketName"); err != nil {
			w.fail("bucket", invalid, "create with an invalid name: %v", err)
		}
	default:
		w.scratchLife(ctx)
	}
}

// scratchLife creates the worker's scratch bucket, puts an object in it,
// and deletes them again, checking the errors on the way
func (w *worker) scratchLife(ctx context.Context) {
	name := w.s.scratchBucket(w.id)
	bucket, key := &name, aws.String(scratchKey)
	fail := func(step string, err error) {
		w.fail("bucket", name, "%s: %v", step, err)
		w.s.removeScratch(ctx, name)
	}

	if _, err := w.s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}); err != nil {
		fail("create", err)
		return
	}
	_, err := w.s.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket})
	if err := expectError(err, http.StatusConflict, "BucketAlreadyOwnedByYou"); err != nil {
		fail("create again", err)
		return
	}
	if _, err := w.s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket}); err != nil {
		fail("head", err)
		return
	}
	data := []byte(name)
	w.wrote(int64(len(data)))
	if _, err := w.s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: bucket, Key: key, Body: bytes.NewReader(data)}); err != nil {
		fail("put", err)
		return
	}
	out, err := w.s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket})
	if err != nil {
		fail("list", err)
		return
	}
	if len(out.Contents) != 1 || aws.ToString(out.Contents[0].Key) != scratchKey {
		fail("list", fmt.Errorf("%d keys listed, want only %s", len(out.Contents), scratchKey))
		return
	}
	// The soak bucket's keys must not be visible here, nor this one there
	_, err = w.s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &w.s.cfg.bucket, Key: key})
	if err := expectError(err, http.StatusNotFound, ""); err != nil {
		fail("head in the soak bucket", err)
		return
	}
	_, err = w.s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket})
	if err := expectError(err, http.StatusConflict, "BucketNotEmpty"); err != nil {
		fail("delete while not empty", err)
		return
	}
	if _, err := w.s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: bucket, Key: key}); err != nil {
		fail("delete object", err)
		return
	}
	if _, err := w.s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}); err != nil {
		fail("delete", err)
		return
	}
	_, err = w.s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket})
	if err := expectError(err, http.StatusNotFound, ""); err != nil {
		w.fail("bucket", name, "head after delete: %v", err)
	}
	_, err = w.s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket})
	if err := expectError(err, http.StatusNotFound, "NoSuchBucket"); err != nil {
		w.fail("bucket", name, "delete again: %v", err)
	}
}

// removeScratch deletes a scratch bucket and its object, if they exist
func (s *soak) removeScratch(ctx context.Context, name string) {
	_, _ = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &name, Key: aws.String(scratchKey)})
	_, _ = s.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &name})
}

// authChecks sends requests that must be refused: writes with the
// read-only credentials, anonymous requests, a request signed with the
// wrong secret and an expired presigned URL. Reads with the read-only
// credentials must work. Refused writes must leave the key as it was.
func (w *worker) authChecks(ctx context.Context) {
	key := w.pickPlainKey()
	n := 4
	if w.s.roClient != nil {
		n = 8
	}
	switch w.rng.IntN(n) {
	case 0:
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, w.s.cfg.endpoint+"/"+w.s.cfg.bucket+"/"+key, nil)
		resp, err := w.s.httpClient.Do(req)
		w.expectRefused("anonymous get", key, resp, err, http.StatusForbidden, "AccessDenied")
	case 1:
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, w.s.cfg.endpoint+"/"+w.s.cfg.bucket+"/"+key, strings.NewReader("anonymous"))
		resp, err := w.s.rawClient.Do(req)
		w.expectRefused("anonymous put", key, resp, err, http.StatusForbidden, "AccessDenied")
		w.verifyKey(ctx, "auth", key)
	case 2:
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, w.s.cfg.endpoint+"/"+w.s.cfg.bucket+"/"+key, nil)
		req.Header.Set("X-Amz-Content-Sha256", emptySHA256)
		creds := w.s.creds
		creds.SecretAccessKey += "wrong"
		// A signer of its own: the signer caches the signing key by access
		// key, so the shared one would sign with the right secret
		if err := v4.NewSigner().SignHTTP(ctx, creds, req, emptySHA256, "s3", region, time.Now()); err != nil {
			w.fail("auth", key, "sign: %v", err)
			return
		}
		resp, err := w.s.rawClient.Do(req)
		w.expectRefused("wrong secret", key, resp, err, http.StatusForbidden, "SignatureDoesNotMatch")
		w.verifyKey(ctx, "auth", key)
	case 3:
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, w.s.cfg.endpoint+"/"+w.s.cfg.bucket+"/"+key+"?X-Amz-Expires=60", nil)
		url, _, err := w.s.signer.PresignHTTP(ctx, w.s.creds, req, "UNSIGNED-PAYLOAD", "s3", region, time.Now().Add(-2*time.Minute))
		if err != nil {
			w.fail("auth", key, "presign: %v", err)
			return
		}
		get, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := w.s.httpClient.Do(get)
		w.expectRefused("expired presigned get", key, resp, err, http.StatusForbidden, "ExpiredToken")
	case 4:
		w.verifyKeyWith(ctx, w.s.roClient, "auth", key)
	case 5:
		_, err := w.s.roClient.PutObject(ctx, &s3.PutObjectInput{Bucket: &w.s.cfg.bucket, Key: &key, Body: strings.NewReader("read-only")})
		if err := expectError(err, http.StatusForbidden, "AccessDenied"); err != nil {
			w.fail("auth", key, "read-only put: %v", err)
			w.resync(ctx, key)
			return
		}
		w.verifyKey(ctx, "auth", key)
	case 6:
		_, err := w.s.roClient.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &w.s.cfg.bucket, Key: &key})
		if err := expectError(err, http.StatusForbidden, "AccessDenied"); err != nil {
			w.fail("auth", key, "read-only delete: %v", err)
			w.resync(ctx, key)
			return
		}
		w.verifyKey(ctx, "auth", key)
	default:
		_, err := w.s.roClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &w.s.cfg.bucket, Key: &key})
		if err := expectError(err, http.StatusForbidden, "AccessDenied"); err != nil {
			w.fail("auth", key, "read-only create multipart upload: %v", err)
		}
		if _, err := w.s.roClient.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: &w.s.cfg.bucket, Prefix: &w.prefix}); err != nil {
			w.fail("auth", key, "read-only list uploads: %v", err)
		}
		if _, err := w.s.roClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: &w.s.cfg.bucket, Prefix: &w.prefix}); err != nil {
			w.fail("auth", key, "read-only list: %v", err)
		}
	}
}

// expectRefused checks that a raw request was answered with the given
// status and error code
func (w *worker) expectRefused(what, key string, resp *http.Response, err error, status int, code string) {
	if err != nil {
		w.fail("auth", key, "%s: %v", what, err)
		return
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != status || !strings.Contains(string(body), "<Code>"+code+"</Code>") {
		w.fail("auth", key, "%s: status %d, want %d %s: %s", what, resp.StatusCode, status, code, body)
	}
}

// health checks the health endpoints
func (w *worker) health(ctx context.Context) {
	for _, path := range []string{"/healthz", "/readyz"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, w.s.cfg.endpoint+path, nil)
		resp, err := w.s.httpClient.Do(req)
		if err != nil {
			w.fail("health", path, "%v", err)
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			w.fail("health", path, "status %d", resp.StatusCode)
		}
	}
}
