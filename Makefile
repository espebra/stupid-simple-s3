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

FUZZ_DURATION ?= 1h
SHORTFUZZ_DURATION ?= 30s

# Fuzz targets: package:FuncName
FUZZ_TARGETS = \
	./internal/auth/:FuzzParseAuthorization \
	./internal/auth/:FuzzParsePresignedURL \
	./internal/auth/:FuzzURIEncode \
	./internal/auth/:FuzzIsValidIPHeader \
	./internal/auth/:FuzzBuildCanonicalQueryStringSingle \
	./internal/auth/:FuzzBuildCanonicalQueryStringMultiple \
	./internal/auth/:FuzzGetPresignedAccessKeyID \
	./internal/auth/:FuzzBuildCanonicalHeaders \
	./internal/api/:FuzzAWSChunkedReader

# Run fuzz targets, then replay any corpus failures found.
# go test -fuzz exits non-zero on timeout, so we ignore its exit code
# and rely on the final replay pass to detect real failures.
fuzz:
	@for target in $(FUZZ_TARGETS); do \
		pkg=$${target%%:*}; func=$${target##*:}; \
		echo "--- fuzzing $$func ($$pkg) ---"; \
		go test -fuzz=$$func -fuzztime=$(FUZZ_DURATION) $$pkg || true; \
	done
	@echo "--- replaying corpus ---"
	go test ./internal/auth/ ./internal/api/

shortfuzz:
	@$(MAKE) fuzz FUZZ_DURATION=$(SHORTFUZZ_DURATION)

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
