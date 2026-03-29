.PHONY: build test docker clean

# Build all packages
build:
	go build ./...

# Release with goreleaser (dry run)
release-dry:
	goreleaser release --snapshot --clean

# Run all tests
test:
	go clean --testcache && go test -v ./...

# Start infrastructure with Docker Compose
docker:
	docker compose up -d

# Clean build artifacts (if any).
clean:
	go clean
