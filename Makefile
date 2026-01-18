.PHONY: build run test bench clean vendor fmt fmt-check

BINARY_NAME=stupid-simple-s3
BUILD_DIR=bin

build:
	go build -mod=vendor -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/sss

run: build
	./$(BUILD_DIR)/$(BINARY_NAME) -config config.yaml

test:
	go test -mod=vendor -v ./...

bench:
	go test -mod=vendor -bench=. -benchmem ./...

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
	mkdir -p /var/lib/sss/data /var/lib/sss/tmp

# Build for multiple platforms
build-all:
	GOOS=linux GOARCH=amd64 go build -mod=vendor -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 ./cmd/sss
	GOOS=linux GOARCH=arm64 go build -mod=vendor -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 ./cmd/sss
	GOOS=darwin GOARCH=amd64 go build -mod=vendor -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 ./cmd/sss
	GOOS=darwin GOARCH=arm64 go build -mod=vendor -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 ./cmd/sss
