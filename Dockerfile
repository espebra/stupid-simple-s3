FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy dependency files first for better caching
COPY go.mod go.sum ./
COPY vendor/ vendor/

# Copy source code
COPY cmd/ cmd/
COPY internal/ internal/

# Build static binary
RUN CGO_ENABLED=0 go build -mod=vendor -ldflags="-s -w" -o stupid-simple-s3 ./cmd/sss

# Create data directories
RUN mkdir -p /var/lib/stupid/data /var/lib/stupid/tmp

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /build/stupid-simple-s3 /stupid-simple-s3
COPY --from=builder --chown=nonroot:nonroot /var/lib/stupid /var/lib/stupid

USER nonroot:nonroot

EXPOSE 5553

ENTRYPOINT ["/stupid-simple-s3"]
