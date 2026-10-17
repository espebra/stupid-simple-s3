[![CI](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml/badge.svg)](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml)
[![Release](https://github.com/espebra/stupid-simple-s3/actions/workflows/release.yaml/badge.svg)](https://github.com/espebra/stupid-simple-s3/actions/workflows/release.yaml)
[![Go Reference](https://pkg.go.dev/badge/github.com/espebra/stupid-simple-s3.svg)](https://pkg.go.dev/github.com/espebra/stupid-simple-s3)

# Stupid Simple S3

A minimal S3-compatible object storage service in Go. Designed for single-server, single-drive deployments where simplicity matters more than redundancy.

## Features

- Simple filesystem-backed storage. No database.
- AWS Signature v4 authentication.
- Read-only and read-write S3 credentials.
- Multi-bucket support via S3 CreateBucket/DeleteBucket API.
- Presigned URL support for temporary access.
- Multipart upload support.
- SHA-256 checksums (`x-amz-checksum-sha256`) verified on upload and returned on download.
- Prometheus metrics endpoint on a separate listener with optional basic auth.
- Few external dependencies.

Note that Stupid Simple S3 is HTTP only. TLS support needs to be provided by a separate TLS proxy.

## Getting started

### Running with Docker

```bash
docker run -d \
  -p 5553:5553 \
  -e STUPID_RW_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE" \
  -e STUPID_RW_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /path/to/data:/var/lib/stupid-simple-s3 \
  ghcr.io/espebra/stupid-simple-s3:latest
```

### Running with Docker Compose

```yaml
services:
  stupid-simple-s3:
    image: ghcr.io/espebra/stupid-simple-s3:latest
    ports:
      - "5553:5553"
    environment:
      STUPID_RW_ACCESS_KEY: "AKIAIOSFODNN7EXAMPLE"
      STUPID_RW_SECRET_KEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    volumes:
      - s3-data:/var/lib/stupid-simple-s3

volumes:
  s3-data:
```

### Installing from packages

```bash
## 1. Install package
# Debian/Ubuntu
sudo dpkg -i stupid-simple-s3_1.0.0_amd64.deb

# RHEL/Fedora
sudo rpm -i stupid-simple-s3-1.0.0.x86_64.rpm

## 2. Configure
# Edit /etc/stupid-simple-s3/environment

## 3. Start now and at boot
sudo systemctl enable --now stupid-simple-s3
```

## Building from source

```bash
# Build on your architecture and platform
make build

# Or build for multiple architectures and platforms
make build-all
```

## Environment variables for configuration

The service is configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `STUPID_HOST` | Listen host | (all interfaces) |
| `STUPID_PORT` | Listen port | `5553` |
| `STUPID_BUCKET_NAME` | Bucket to auto-create at startup | (optional) |
| `STUPID_STORAGE_PATH` | Storage path for objects | `/var/lib/stupid-simple-s3/data` |
| `STUPID_MULTIPART_PATH` | Temporary storage path for multipart uploads during upload | `/var/lib/stupid-simple-s3/tmp` |
| `STUPID_CLEANUP_ENABLED` | Enable cleanup job (`true`/`false`) | `true` |
| `STUPID_CLEANUP_INTERVAL` | Cleanup interval | `1h` |
| `STUPID_CLEANUP_MAX_AGE` | Max age for stale uploads | `24h` |
| `STUPID_RO_ACCESS_KEY` | Read-only user access key | (optional) |
| `STUPID_RO_SECRET_KEY` | Read-only user secret key | (optional) |
| `STUPID_RW_ACCESS_KEY` | Read-write user access key | (optional) |
| `STUPID_RW_SECRET_KEY` | Read-write user secret key | (optional) |
| `STUPID_METRICS_ADDRESS` | Address for the metrics listener serving `/metrics` (e.g. `:5554`). Empty disables `/metrics`. | (disabled) |
| `STUPID_METRICS_USERNAME` | Username for /metrics (and pprof) basic auth | (optional) |
| `STUPID_METRICS_PASSWORD` | Password for /metrics (and pprof) basic auth | (optional) |
| `STUPID_MAX_OBJECT_SIZE` | Maximum object size in bytes | `5368709120` (5GB) |
| `STUPID_MAX_PART_SIZE` | Maximum multipart part size in bytes | `5368709120` (5GB) |
| `STUPID_MAX_CHUNK_SIZE` | Maximum AWS chunked encoding chunk size in bytes | `5368709120` (5GB) |
| `STUPID_TRUSTED_PROXIES` | Comma-separated list of trusted proxy IPs/CIDRs to read the X-Real-IP header from | (optional) |
| `STUPID_READ_TIMEOUT` | Maximum duration for reading requests | `30m` |
| `STUPID_WRITE_TIMEOUT` | Maximum duration for writing responses | `30m` |
| `STUPID_SHUTDOWN_TIMEOUT` | Maximum duration for graceful shutdown | `30s` |
| `STUPID_LOG_FORMAT` | Log output format (`text` or `json`) | `text` |
| `STUPID_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | `info` |
| `STUPID_PPROF_ENABLED` | Serve pprof endpoints on the metrics listener (`true`/`false`). Requires `STUPID_METRICS_ADDRESS`. | `false` |

At least one credential pair (read-only or read-write) must be provided.

### Graceful Shutdown

When the server receives a shutdown signal (SIGINT or SIGTERM), it performs a graceful shutdown:

1. **New requests are rejected** - The server immediately stops accepting new connections. Clients attempting to connect will receive a connection refused error.
2. **In-flight requests are allowed to complete** - Existing requests continue processing until they finish or the shutdown timeout is reached.
3. **Timeout enforcement** - If in-flight requests don't complete within `STUPID_SHUTDOWN_TIMEOUT` (default: 30 seconds), the server forcefully terminates remaining connections.

This behavior ensures that ongoing uploads and downloads have a chance to complete during deployments or restarts, while preventing the server from hanging indefinitely on stuck connections.

### Cleanup Job

The cleanup job runs periodically to remove stale multipart uploads. When a multipart upload is initiated but never completed or aborted, the uploaded parts remain on disk in the `STUPID_MULTIPART_PATH` directory. The cleanup job deletes these orphaned uploads to reclaim disk space.

It also removes temp files left in a bucket's `tmp/` directory by uploads that were interrupted by a crash or restart. Only the `tmp/` directories are read, so the cost of the cleanup job does not grow with the number of objects. Versions before the `tmp/` directory kept temp files next to the objects, as `data.tmp.*` and `meta.json.tmp.*`; leftovers from those versions are not removed by the cleanup job, and can be removed once with, for example, `find /var/lib/stupid-simple-s3/data/buckets -name '*.tmp.*' -mmin +60 -delete`.

- **Interval**: How often the cleanup job runs (default: every hour)
- **Max Age**: Uploads older than this are considered stale and removed (default: 24 hours)

Set `STUPID_CLEANUP_ENABLED=false` to disable the cleanup job entirely.

## Usage with AWS CLI

Configure AWS CLI to use your credentials:

```bash
aws configure set aws_access_key_id AKIAIOSFODNN7EXAMPLE
aws configure set aws_secret_access_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws configure set default.region us-east-1
```

Basic operations:

```bash
# Create a bucket
aws --endpoint-url http://localhost:5553 s3 mb s3://my-bucket

# Delete an empty bucket
aws --endpoint-url http://localhost:5553 s3 rb s3://my-bucket

# Upload a file
aws --endpoint-url http://localhost:5553 s3 cp file.txt s3://my-bucket/file.txt

# Download a file
aws --endpoint-url http://localhost:5553 s3 cp s3://my-bucket/file.txt -

# Delete a file
aws --endpoint-url http://localhost:5553 s3 rm s3://my-bucket/file.txt
```

## Presigned URLs

Generate presigned URLs to grant temporary access to objects without sharing credentials:

```bash
# Generate a presigned URL for downloading (valid for 1 hour)
aws --endpoint-url http://localhost:5553 s3 presign s3://my-bucket/file.txt --expires-in 3600
```

The generated URL can be used directly with `curl` or any HTTP client:

```bash
# Download using presigned URL
curl -o file.txt "http://localhost:5553/my-bucket/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&..."

# Upload using presigned URL
curl -X PUT -T file.txt "http://localhost:5553/my-bucket/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&..."
```

Presigned URL parameters:
- Maximum expiry time: 7 days (604800 seconds)
- Supported operations: GET (download), PUT (upload), HEAD, DELETE

The access log replaces the values of all `X-Amz-*` query parameters with `REDACTED`, so presigned URLs found in the logs cannot be used.

## Checksums and ETags

Uploads are verified when the client sends a SHA-256 checksum (`x-amz-checksum-sha256`), either as a header or as an aws-chunked trailer. An upload whose data does not match is rejected with `400 BadDigest` and is not stored. Checksums using other algorithms are accepted but not verified. AWS SDKs send CRC32 by default, so clients must ask for SHA-256 to get their uploads verified:

```bash
# AWS CLI
aws --endpoint-url http://localhost:5553 s3 cp file.txt s3://my-bucket/file.txt --checksum-algorithm SHA256
```

```go
// AWS SDK for Go v2: set ChecksumAlgorithm on PutObject, CreateMultipartUpload and UploadPart
client.PutObject(ctx, &s3.PutObjectInput{
	Bucket:            aws.String("my-bucket"),
	Key:               aws.String("file.txt"),
	Body:              body,
	ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
})
```

Other SDKs have the same setting, for example `ChecksumAlgorithm='SHA256'` in boto3.

PutObject, GetObject, HeadObject, UploadPart and CompleteMultipartUpload return the stored checksum in `x-amz-checksum-sha256`, with its type in `x-amz-checksum-type`:

- `FULL_OBJECT` for objects uploaded with PutObject: the base64 SHA-256 of the object data. AWS SDKs validate downloads against it when response checksum validation is enabled, which is their default.
- `COMPOSITE` for multipart objects: the base64 SHA-256 of the concatenated binary SHA-256 digests of the parts, followed by `-N` where N is the number of parts. SDKs cannot recompute it from the object data and skip validating it.

Request bodies are also checked against the request signature. When `X-Amz-Content-Sha256` holds the SHA-256 of the body, a body that does not match is rejected with `400 XAmzContentSHA256Mismatch`. For signed aws-chunked uploads (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`), every chunk signature and the trailer signature are verified, and a mismatch is rejected with `403 SignatureDoesNotMatch`. Requests sent with `UNSIGNED-PAYLOAD`, which is what AWS SDKs use over HTTPS, only have their body verified if they include an `x-amz-checksum-sha256` checksum, so the connection between the TLS proxy and the service must be trusted for other uploads.

Range requests do not return a checksum. Objects stored before SHA-256 checksums were introduced have none.

ETags are derived from the same digests: `"sha256-<hex>"` for objects and parts, and `"sha256-<hex>-N"` for multipart objects. They depend only on the content, so uploading identical data again keeps the ETag. They are not MD5 digests, so tools that compare ETags with a locally computed MD5 cannot use them. The `-` is intentional: AWS SDK for Java v1 treats an ETag without one as an MD5 digest and fails to validate it.

## Supported S3 Operations

| Operation | Method | Path |
|-----------|--------|------|
| CreateBucket | PUT | `/{bucket}` |
| DeleteBucket | DELETE | `/{bucket}` |
| HeadBucket | HEAD | `/{bucket}` |
| GetBucketLocation | GET | `/{bucket}?location` |
| ListObjectsV2 | GET | `/{bucket}?list-type=2` |
| PutObject | PUT | `/{bucket}/{key}` |
| CopyObject | PUT | `/{bucket}/{key}` with `x-amz-copy-source` header |
| GetObject | GET | `/{bucket}/{key}` |
| GetObject (Range) | GET | `/{bucket}/{key}` with `Range` header |
| HeadObject | HEAD | `/{bucket}/{key}` |
| DeleteObject | DELETE | `/{bucket}/{key}` |
| DeleteObjects | POST | `/{bucket}?delete` |
| CreateMultipartUpload | POST | `/{bucket}/{key}?uploads` |
| UploadPart | PUT | `/{bucket}/{key}?partNumber=N&uploadId=X` |
| CompleteMultipartUpload | POST | `/{bucket}/{key}?uploadId=X` |
| AbortMultipartUpload | DELETE | `/{bucket}/{key}?uploadId=X` |
| ListMultipartUploads | GET | `/{bucket}?uploads` |
| ListParts | GET | `/{bucket}/{key}?uploadId=X` |

Notes:
- GetBucketLocation returns an empty location constraint, which S3 clients read as `us-east-1`. The service has no regions.
- As in S3, every part of a multipart upload except the last must be at least 5 MiB. CompleteMultipartUpload fails with `EntityTooSmall` otherwise.
- ListMultipartUploads supports `prefix`, `key-marker`, `upload-id-marker` and `max-uploads`. `delimiter` is ignored.
- ListParts supports `part-number-marker` and `max-parts`.
- GetObject and HeadObject support the conditional headers `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since`, answering `304 Not Modified` or `412 Precondition Failed` as S3 does. A 304 response does not read the object data, so a cache such as Varnish can revalidate objects cheaply. Conditional writes are not supported.

## Health check endpoints

| Endpoint | Description |
|----------|-------------|
| `/healthz` | Liveness probe - returns 200 OK if the server is running |
| `/readyz` | Readiness probe - returns 200 OK if the server is ready to accept requests |

These endpoints do not require authentication.

## Metrics endpoint

| Endpoint | Description |
|----------|-------------|
| `/metrics` | Prometheus metrics |

`/metrics` is served only on a separate metrics listener, which keeps it off the port that clients (and Varnish) reach. It is disabled unless `STUPID_METRICS_ADDRESS` is set:

```bash
export STUPID_METRICS_ADDRESS=:5554
```

On the S3 listener, `/metrics` is an ordinary S3 path (the bucket `metrics`). The health check endpoints stay on the S3 listener.

By default, no authentication is required. To enable basic authentication, set both `STUPID_METRICS_USERNAME` and `STUPID_METRICS_PASSWORD` environment variables.

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `stupid_simple_s3_build_info` | Gauge | Build information (`version`, `commit` labels), always `1` |
| `stupid_simple_s3_http_requests_in_flight` | Gauge | Number of requests currently being processed |
| `stupid_simple_s3_http_requests_total` | Counter | Total HTTP requests by method, operation, and status |
| `stupid_simple_s3_http_request_duration_seconds` | Histogram | Request latency distribution |
| `stupid_simple_s3_http_request_bytes_total` | Counter | Bytes received in request bodies |
| `stupid_simple_s3_http_response_bytes_total` | Counter | Bytes sent in response bodies |
| `stupid_simple_s3_errors_total` | Counter | Errors by operation and error code |
| `stupid_simple_s3_multipart_uploads_active` | Gauge | Number of active multipart uploads |
| `stupid_simple_s3_uploads_active` | Gauge | Number of currently active upload operations |
| `stupid_simple_s3_downloads_active` | Gauge | Number of currently active download operations |
| `stupid_simple_s3_auth_failures_total` | Counter | Authentication failures by reason |
| `stupid_simple_s3_buckets_total` | Gauge | Current number of buckets |
| `stupid_simple_s3_bucket_creations_total` | Counter | Total bucket creations |
| `stupid_simple_s3_bucket_deletions_total` | Counter | Total bucket deletions |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'stupid-simple-s3'
    static_configs:
      - targets: ['localhost:5554'] # the STUPID_METRICS_ADDRESS port
    # If basic auth is enabled:
    # basic_auth:
    #   username: 'metrics_user'
    #   password: 'metrics_password'
```

Example Kubernetes container port and `PodMonitor` for the metrics listener:

```yaml
# Container spec
env:
  - name: STUPID_METRICS_ADDRESS
    value: ":5554"
ports:
  - name: s3
    containerPort: 5553
  - name: metrics
    containerPort: 5554
---
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: stupid-simple-s3
spec:
  selector:
    matchLabels:
      app: stupid-simple-s3
  podMetricsEndpoints:
    - port: metrics
```

## Profiling with pprof

The standard Go `net/http/pprof` endpoints can be served on the metrics listener. They are disabled by default and never registered on the S3 listener. When metrics basic auth is configured, it also applies to the pprof endpoints.

Enable them together with the metrics listener:

```bash
export STUPID_METRICS_ADDRESS=:5554
export STUPID_PPROF_ENABLED=true
```

Available endpoints (under `/debug/pprof/`):

| Endpoint | Description |
|----------|-------------|
| `/debug/pprof/` | Index of available profiles |
| `/debug/pprof/profile` | CPU profile (default 30s) |
| `/debug/pprof/heap` | Heap allocations |
| `/debug/pprof/goroutine` | Goroutine stacks |
| `/debug/pprof/trace` | Execution trace |
| `/debug/pprof/cmdline`, `/debug/pprof/symbol` | Process metadata |

Example usage from the same host (or via SSH port-forward or `kubectl port-forward`):

```bash
go tool pprof http://127.0.0.1:5554/debug/pprof/profile?seconds=30
go tool pprof http://127.0.0.1:5554/debug/pprof/heap
```

The pprof endpoints expose internal program state and must not be reachable from untrusted networks.

## Filesystem layout for storage

Objects are stored on the filesystem organized by bucket, with a 4-character hash prefix (65,536 directories per bucket) for even distribution. The object directory name is the full SHA-256 hex digest of the key (64 characters), which keeps directory names at a fixed length regardless of key size. The original S3 key is stored in `meta.json`.

```
/var/lib/stupid-simple-s3/data/buckets/
  {bucket-name}/
    objects/
      {4-char-sha256-prefix-of-the-key}/
        {sha256-hex-digest-of-the-key}/
          data        # object content
          meta.json   # metadata (key, size, content-type, etag, etc.)
    tmp/
      {uuid}.data     # temp files of uploads in progress, renamed into
      {uuid}.meta     # the object directory when the upload completes

/var/lib/stupid-simple-s3/tmp/
  {upload-id}/
    meta.json     # upload metadata
    part.00001    # part files
    part.00002
    ...
```

## Limitations

**The first listing of a bucket reads every object's metadata.** Objects are stored under SHA-256 hash directories for even write distribution, so there is no on-disk key ordering. To list a bucket in key order, the service keeps an in-memory index of each bucket's keys, with the size, ETag and last-modified time of each object.

- The index of a bucket is built on its first listing after startup, by walking the bucket and reading every `meta.json`. That listing is O(N) in disk I/O. Buckets that are never listed are never indexed.
- After that, writes and deletes made through the service keep the index up to date, and listings are served from memory with no disk I/O, in time proportional to the page size.
- The index uses roughly 150–200 bytes of memory per object in each listed bucket.
- Changes made directly on disk, outside the service, are not seen by listings until the service is restarted.

## Production Deployment

HTTPS is not supported directly. Use a reverse proxy like Varnish or nginx in front of the service for TLS termination.

When using a reverse proxy, configure `STUPID_TRUSTED_PROXIES` to trust the proxy's IP headers:

```bash
# Trust the local proxy
export STUPID_TRUSTED_PROXIES="127.0.0.1"

# Trust multiple proxies or CIDR ranges
export STUPID_TRUSTED_PROXIES="127.0.0.1,10.0.0.0/8,192.168.1.0/24"
```

When `STUPID_TRUSTED_PROXIES` is configured and a request arrives from one of the trusted IPs:
- The `X-Real-IP` request header is used to determine the original client IP
- The extracted client IP appears in access logs instead of the proxy's IP

Without this configuration, the service ignores the `X-Real-IP` header for security, and access logs will show the proxy's IP address.

## Release process

Releases are automated via GitHub Actions. Pushing a version tag triggers the release workflow.

```bash
git tag v1.0.0
git push origin v1.0.0
```

The release workflow builds and publishes:

| Artifact | Description |
|----------|-------------|
| `stupid-simple-s3-linux-amd64` | Linux binary (x86_64) |
| `stupid-simple-s3-linux-arm64` | Linux binary (ARM64) |
| `stupid-simple-s3-darwin-amd64` | macOS binary (Intel) |
| `stupid-simple-s3-darwin-arm64` | macOS binary (Apple Silicon) |
| `stupid-simple-s3_*_amd64.deb` | Debian/Ubuntu package (x86_64) |
| `stupid-simple-s3_*_arm64.deb` | Debian/Ubuntu package (ARM64) |
| `stupid-simple-s3-*.x86_64.rpm` | RHEL/Fedora package (x86_64) |
| `stupid-simple-s3-*.aarch64.rpm` | RHEL/Fedora package (ARM64) |
| `checksums.txt` | SHA256 checksums |
| Container image | Multi-arch image on ghcr.io |

## Testing

Run the test suite:

```bash
# Run all tests
make test

# Run tests with verbose output
go test -v ./...

# Run benchmark tests
go test -bench=. ./...

# Run benchmarks with memory stats
go test -bench=. -benchmem ./...

# Run fuzz tests
make fuzz
```

## License

BSD 2-Clause
