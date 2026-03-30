.PHONY: build test docker clean release release-dry

VERSION ?= $(shell git describe --tags --always --dirty)

# Build all packages
build:
	go build -ldflags="-X 'github.com/simonmittag/acropora.Version=$(VERSION)'" ./...

# Release with goreleaser
release: build test
	git tag $(VERSION)
	git push origin $(VERSION)

# Release with goreleaser (dry run)
release-dry:
	GORELEASER_CURRENT_TAG=$(VERSION) goreleaser release --snapshot --clean

# Run all tests
test:
	go clean --testcache && go test -v ./...

# Start infrastructure with Docker Compose
docker:
	docker compose up -d

# Clean build artifacts (if any).
clean:
	go clean
