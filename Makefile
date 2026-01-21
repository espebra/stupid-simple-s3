.PHONY: build run test bench fuzz clean vendor fmt fmt-check

BINARY_NAME=stupid-simple-s3
BUILD_DIR=bin

# Version from git tag (commit info comes from Go's built-in build info)
VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "dev")
LDFLAGS = -X github.com/espen/stupid-simple-s3/internal/version.Version=$(VERSION)

build:
	go build -mod=vendor -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/sss

run: build
	./$(BUILD_DIR)/$(BINARY_NAME)

test:
	go test -mod=vendor -v ./...

bench:
	go test -mod=vendor -bench=. -benchmem ./...

fuzz:
	go test -fuzz=FuzzParseAuthorization -fuzztime=1h ./internal/auth/
	go test -fuzz=FuzzParsePresignedURL -fuzztime=1h ./internal/auth/
	go test -fuzz=FuzzURIEncode -fuzztime=1h ./internal/auth/

clean:
	rm -rf $(BUILD_DIR)
	go clean

vendor:
	go mod tidy
	go mod vendor

fmt:
	gofmt -w -s ./cmd ./internal

fmt-check:
	@test -z "$$(gofmt -l -s ./cmd ./internal)" || (echo "Files not formatted:"; gofmt -l -s ./cmd ./internal; exit 1)

# Development helpers
dev-dirs:
	mkdir -p /var/lib/stupid-simple-s3/data /var/lib/stupid-simple-s3/tmp

# Build for multiple platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -mod=vendor -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/sss
	GOOS=linux GOARCH=arm64 go build -mod=vendor -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/sss
	GOOS=darwin GOARCH=amd64 go build -mod=vendor -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/sss
	GOOS=darwin GOARCH=arm64 go build -mod=vendor -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/sss
