# Stupid Simple S3

[![CI](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml/badge.svg)](https://github.com/espebra/stupid-simple-s3/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/espebra/stupid-simple-s3)](https://goreportcard.com/report/github.com/espebra/stupid-simple-s3)
[![codecov](https://codecov.io/gh/espebra/stupid-simple-s3/branch/main/graph/badge.svg)](https://codecov.io/gh/espebra/stupid-simple-s3)
[![Go Reference](https://pkg.go.dev/badge/github.com/espebra/stupid-simple-s3.svg)](https://pkg.go.dev/github.com/espebra/stupid-simple-s3)

A minimal S3-compatible object storage service in Go. Designed for single-server, single-drive deployments where simplicity matters more than redundancy.

## Features

- Single bucket configuration
- AWS Signature v4 authentication
- Read-only and read-write access levels per credential
- Presigned URL support for temporary access
- Multipart upload support
- Filesystem-backed storage
- Prometheus metrics endpoint
- Minimal external dependencies

## Configuration

Create a `config.yaml` file:

```yaml
bucket:
  name: "my-bucket"

storage:
  path: "/var/lib/sss/data"
  multipart_path: "/var/lib/sss/tmp"

server:
  address: ":8080"

credentials:
  - access_key_id: "AKIAIOSFODNN7EXAMPLE"
    secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    privileges: "read-write"

  - access_key_id: "AKIAIOSFODNN7READONLY"
    secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYREADONLY"
    privileges: "read"
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
mkdir -p /var/lib/sss/data /var/lib/sss/tmp

# Run with default config
make run

# Or run directly
./bin/sss -config /path/to/config.yaml
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
aws --endpoint-url http://localhost:8080 s3 cp file.txt s3://my-bucket/file.txt

# Download a file
aws --endpoint-url http://localhost:8080 s3 cp s3://my-bucket/file.txt -

# Delete a file
aws --endpoint-url http://localhost:8080 s3 rm s3://my-bucket/file.txt

# Upload a large file (uses multipart automatically)
aws --endpoint-url http://localhost:8080 s3 cp large-file.bin s3://my-bucket/large-file.bin
```

## Presigned URLs

Generate presigned URLs to grant temporary access to objects without sharing credentials:

```bash
# Generate a presigned URL for downloading (valid for 1 hour)
aws --endpoint-url http://localhost:8080 s3 presign s3://my-bucket/file.txt --expires-in 3600

# Generate a presigned URL for uploading
aws --endpoint-url http://localhost:8080 s3 presign s3://my-bucket/new-file.txt --expires-in 3600
```

The generated URL can be used directly with `curl` or any HTTP client:

```bash
# Download using presigned URL
curl -o file.txt "http://localhost:8080/my-bucket/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&..."

# Upload using presigned URL
curl -X PUT -T file.txt "http://localhost:8080/my-bucket/file.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&..."
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

Prometheus metrics are available at `/metrics` (no authentication required).

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `sss_http_requests_in_flight` | Gauge | Number of requests currently being processed |
| `sss_http_requests_total` | Counter | Total HTTP requests by method, operation, and status |
| `sss_http_request_duration_seconds` | Histogram | Request latency distribution |
| `sss_http_request_bytes_total` | Counter | Bytes received in request bodies |
| `sss_http_response_bytes_total` | Counter | Bytes sent in response bodies |
| `sss_errors_total` | Counter | Errors by operation and error code |
| `sss_auth_failures_total` | Counter | Authentication failures by reason |

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'sss'
    static_configs:
      - targets: ['localhost:8080']
```

## Storage Layout

Objects are stored on the filesystem:

```
/var/lib/sss/data/objects/
  {hash-prefix}/
    {base64-key}/
      data        # object content
      meta.json   # metadata (key, size, content-type, etag, etc.)

/var/lib/sss/tmp/
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
        proxy_pass http://127.0.0.1:8080;
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
    .port = "8080";
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
