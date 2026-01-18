FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy dependency files first for better caching
COPY go.mod go.sum ./
COPY vendor/ vendor/

# Copy source code
COPY cmd/ cmd/
COPY internal/ internal/

# Build static binary
RUN CGO_ENABLED=0 go build -mod=vendor -ldflags="-s -w" -o sss ./cmd/sss

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /build/sss /sss

USER nonroot:nonroot

EXPOSE 5553

ENTRYPOINT ["/sss"]
CMD ["-config", "/etc/sss/config.yaml"]
