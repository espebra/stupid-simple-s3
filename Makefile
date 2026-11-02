.PHONY: build run test bench fuzz shortfuzz soak clean vendor fmt fmt-check

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
# Workers per fuzz target. Empty means GOMAXPROCS. Set it when running
# targets in parallel, e.g. make -j16 fuzz FUZZ_PARALLEL=8 on 128 cores.
FUZZ_PARALLEL ?=

FUZZ_AUTH = FuzzParseAuthorization FuzzParsePresignedURL FuzzURIEncode \
	FuzzIsValidIPHeader FuzzBuildCanonicalQueryStringSingle \
	FuzzBuildCanonicalQueryStringMultiple FuzzGetPresignedAccessKeyID \
	FuzzBuildCanonicalHeaders
FUZZ_API = FuzzAWSChunkedReader FuzzRangeRequest FuzzConditionalHeaders \
	FuzzDeleteObjectsBody FuzzCompleteMultipartUploadBody
FUZZ_STORAGE = FuzzValidateBucketName FuzzKeyToPath FuzzUploadDir

fuzz_pkg = $(if $(filter $(1),$(FUZZ_AUTH)),./internal/auth/,$(if $(filter $(1),$(FUZZ_API)),./internal/api/,$(if $(filter $(1),$(FUZZ_STORAGE)),./internal/storage/,$(error unknown fuzz target $(1)))))

# Run every fuzz target for FUZZ_DURATION, one after another, or in parallel
# with make -j. A target that finds a failure exits non-zero, which stops
# make, and writes the failing input to testdata/fuzz, where go test replays
# it from then on. Reaching FUZZ_DURATION exits zero.
fuzz: $(addprefix fuzz-,$(FUZZ_AUTH) $(FUZZ_API) $(FUZZ_STORAGE))

fuzz-%:
	go test -mod=vendor -run='^$$' -fuzz='^$*$$' -fuzztime=$(FUZZ_DURATION) $(if $(FUZZ_PARALLEL),-parallel=$(FUZZ_PARALLEL)) $(call fuzz_pkg,$*)

shortfuzz:
	@$(MAKE) fuzz FUZZ_DURATION=$(SHORTFUZZ_DURATION)

SOAK_DURATION ?= 1h
SOAK_FLAGS ?=

# Run a mixed S3 workload against a freshly started server, checking every
# object read back and that the server leaks no goroutines, fds, memory,
# temp files or uploads. See tests/soak/main.go for the flags.
soak: build
	go run -mod=vendor ./tests/soak -server $(BUILD_DIR)/$(BINARY_NAME) -duration $(SOAK_DURATION) $(SOAK_FLAGS)

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
