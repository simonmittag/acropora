.PHONY: build test docker clean

# Build all packages
build:
	go build ./...

# Run all tests
test:
	go clean --testcache && go test -v ./...

# Start infrastructure with Docker Compose
docker:
	docker compose up -d

# Clean build artifacts (if any).
clean:
	go clean
