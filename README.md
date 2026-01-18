# Stupid Simple S3

[![CI](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml/badge.svg)](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/espebra/stupid-simple-s3)](https://goreportcard.com/report/github.com/espebra/stupid-simple-s3)
[![Go Reference](https://pkg.go.dev/badge/github.com/espebra/stupid-simple-s3.svg)](https://pkg.go.dev/github.com/espebra/stupid-simple-s3)

A minimal S3-compatible object storage service in Go. Designed for single-server, single-drive deployments where simplicity matters more than redundancy.

## Features

- Single bucket configuration
- AWS Signature v4 authentication
- Read-only and read-write S3 credentials
- Presigned URL support for temporary access
- Multipart upload support
- Filesystem-backed storage
- Prometheus metrics endpoint with optional basic auth
- Minimal external dependencies

## Configuration

The service is configured using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `STUPID_HOST` | Listen host | (all interfaces) |
| `STUPID_PORT` | Listen port | `5553` |
| `STUPID_BUCKET_NAME` | Bucket name | (required) |
| `STUPID_STORAGE_PATH` | Storage path for objects | `/var/lib/stupid/data` |
| `STUPID_MULTIPART_PATH` | Storage path for multipart uploads | `/var/lib/stupid/tmp` |
| `STUPID_CLEANUP_ENABLED` | Enable cleanup job (`true`/`false`) | `true` |
| `STUPID_CLEANUP_INTERVAL` | Cleanup interval | `1h` |
| `STUPID_CLEANUP_MAX_AGE` | Max age for stale uploads | `24h` |
| `STUPID_RO_ACCESS_KEY` | Read-only user access key | (optional) |
| `STUPID_RO_SECRET_KEY` | Read-only user secret key | (optional) |
| `STUPID_RW_ACCESS_KEY` | Read-write user access key | (optional) |
| `STUPID_RW_SECRET_KEY` | Read-write user secret key | (optional) |
| `STUPID_METRICS_USERNAME` | Username for /metrics basic auth | (optional) |
| `STUPID_METRICS_PASSWORD` | Password for /metrics basic auth | (optional) |

At least one credential pair (read-only or read-write) must be provided.

Example:

```bash
export STUPID_BUCKET_NAME="my-bucket"
export STUPID_STORAGE_PATH="/var/lib/stupid-simple-s3/data"
export STUPID_MULTIPART_PATH="/var/lib/stupid-simple-s3/tmp"
export STUPID_RW_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export STUPID_RW_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
./bin/stupid-simple-s3
```

## Building

```bash
# Build (uses vendored dependencies)
make build

# Or build for multiple platforms
make build-all

# Update vendored dependencies
make vendor
```

## Running

```bash
# Create storage directories
mkdir -p /var/lib/stupid-simple-s3/data /var/lib/stupid-simple-s3/tmp

# Set required environment variables and run
export STUPID_BUCKET_NAME="my-bucket"
export STUPID_STORAGE_PATH="/var/lib/stupid-simple-s3/data"
export STUPID_MULTIPART_PATH="/var/lib/stupid-simple-s3/tmp"
export STUPID_RW_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE"
export STUPID_RW_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
./bin/stupid-simple-s3
```

## Usage with AWS CLI

Configure AWS CLI to use your credentials:

```bash
aws configure set aws_access_key_id AKIAIOSFODNN7EXAMPLE
aws configure set aws_secret_access_key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws configure set default.region us-east-1
```

Basic operations:

```bash
# Upload a file
aws --endpoint-url http://localhost:5553 s3 cp file.txt s3://my-bucket/file.txt

# Download a file
aws --endpoint-url http://localhost:5553 s3 cp s3://my-bucket/file.txt -

# Delete a file
aws --endpoint-url http://localhost:5553 s3 rm s3://my-bucket/file.txt

# Upload a large file (uses multipart automatically)
aws --endpoint-url http://localhost:5553 s3 cp large-file.bin s3://my-bucket/large-file.bin
```

## Presigned URLs

Generate presigned URLs to grant temporary access to objects without sharing credentials:

```bash
# Generate a presigned URL for downloading (valid for 1 hour)
aws --endpoint-url http://localhost:5553 s3 presign s3://my-bucket/file.txt --expires-in 3600

# Generate a presigned URL for uploading
aws --endpoint-url http://localhost:5553 s3 presign s3://my-bucket/new-file.txt --expires-in 3600
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

## Supported S3 Operations

| Operation | Method | Path |
|-----------|--------|------|
| HeadBucket | HEAD | `/{bucket}` |
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

## Metrics

Prometheus metrics are available at `/metrics`. By default, no authentication is required. To enable basic authentication, set both `STUPID_METRICS_USERNAME` and `STUPID_METRICS_PASSWORD` environment variables.

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `stupid_simple_s3_http_requests_in_flight` | Gauge | Number of requests currently being processed |
| `stupid_simple_s3_http_requests_total` | Counter | Total HTTP requests by method, operation, and status |
| `stupid_simple_s3_http_request_duration_seconds` | Histogram | Request latency distribution |
| `stupid_simple_s3_http_request_bytes_total` | Counter | Bytes received in request bodies |
| `stupid_simple_s3_http_response_bytes_total` | Counter | Bytes sent in response bodies |
| `stupid_simple_s3_errors_total` | Counter | Errors by operation and error code |
| `stupid_simple_s3_auth_failures_total` | Counter | Authentication failures by reason |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'sss'
    static_configs:
      - targets: ['localhost:5553']
    # If basic auth is enabled:
    # basic_auth:
    #   username: 'metrics_user'
    #   password: 'metrics_password'
```

## Storage Layout

Objects are stored on the filesystem:

```
/var/lib/stupid-simple-s3/data/objects/
  {hash-prefix}/
    {base64-key}/
      data        # object content
      meta.json   # metadata (key, size, content-type, etag, etc.)

/var/lib/stupid-simple-s3/tmp/
  {upload-id}/
    meta.json     # upload metadata
    part.00001    # part files
    part.00002
    ...
```

## Production Deployment

HTTPS is not supported directly. Use a reverse proxy like Varnish or nginx in front of the service for TLS termination.

Example nginx configuration:

```nginx
server {
    listen 443 ssl;
    server_name s3.example.com;

    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;

    location / {
        proxy_pass http://127.0.0.1:5553;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Example Varnish VCL configuration (use with [hitch](https://github.com/varnish/hitch) for TLS termination):

```vcl
vcl 4.1;

backend default {
    .host = "127.0.0.1";
    .port = "5553";
}

sub vcl_recv {
    # Pass all requests to the backend (no caching for S3 operations)
    return (pass);
}

sub vcl_backend_response {
    # Do not cache responses
    set beresp.uncacheable = true;
}
```

## Releasing

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

### Installing from packages

```bash
# Debian/Ubuntu
sudo dpkg -i stupid-simple-s3_1.0.0_amd64.deb
sudo systemctl edit stupid-simple-s3  # Add environment variables
sudo systemctl enable --now stupid-simple-s3

# RHEL/Fedora
sudo rpm -i stupid-simple-s3-1.0.0.x86_64.rpm
sudo systemctl edit stupid-simple-s3  # Add environment variables
sudo systemctl enable --now stupid-simple-s3
```

### Running with Docker

```bash
docker run -d \
  -p 5553:5553 \
  -e STUPID_BUCKET_NAME="my-bucket" \
  -e STUPID_STORAGE_PATH="/data" \
  -e STUPID_MULTIPART_PATH="/tmp" \
  -e STUPID_RW_ACCESS_KEY="AKIAIOSFODNN7EXAMPLE" \
  -e STUPID_RW_SECRET_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
  -v /path/to/data:/data \
  -v /path/to/tmp:/tmp \
  ghcr.io/espebra/stupid-simple-s3:latest
```

## Testing

Run the test suite:

```bash
# Run all tests
make test

# Run tests with verbose output
go test -v ./...

# Run benchmarks
go test -bench=. ./...

# Run benchmarks with memory stats
go test -bench=. -benchmem ./...
```

## License

BSD 2-Clause
