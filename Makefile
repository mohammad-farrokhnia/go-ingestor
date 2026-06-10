.PHONY: build run \
        test test-race test-integration test-all \
        coverage lint fmt vet \
        proto \
        docker-build docker-run \
        infra-up infra-down infra-logs \
        clean help

VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"


build:
	go build $(LDFLAGS) -o bin/ingestor ./cmd/ingestor

run:
	go run $(LDFLAGS) ./cmd/ingestor


## Unit tests (no external services required). Cached runs are skipped.
test:
	go test -count=1 ./...

## Unit tests with the race detector enabled.
test-race:
	go test -race -count=1 ./...

## Integration tests only. Always runs fresh (-count=1), verbose, with race
## detector and a 2-minute safety timeout.
test-integration:
	go test -tags=integration -v -race -count=1 -timeout=120s ./internal/integration/...

## Run unit tests followed by integration tests.
test-all: test test-integration

## Generate an HTML coverage report from unit tests.
coverage:
	go test -count=1 -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Report written to coverage.html"


lint:
	golangci-lint run ./...

fmt:
	gofmt -w -s .

vet:
	go vet ./...


proto:
	protoc --go_out=paths=source_relative:./proto \
	       --go-grpc_out=paths=source_relative:./proto \
	       --proto_path=proto proto/ingestor/v1/ingestor.proto


docker-build:
	docker build -t go-ingestor:$(VERSION) -f deployments/docker/Dockerfile .

docker-run:
	docker run --rm \
	  -p 50051:50051 \
	  -p 8080:8080 \
	  -v $(PWD)/configs:/app/configs:ro \
	  go-ingestor:$(VERSION)


infra-up:
	docker compose -f deployments/docker/docker-compose.yaml up -d

infra-down:
	docker compose -f deployments/docker/docker-compose.yaml down

infra-logs:
	docker compose -f deployments/docker/docker-compose.yaml logs -f kafka


clean:
	go clean
	rm -rf bin/ dist/ coverage.out coverage.html


help:
	@printf "\nUsage: make <target>\n\n"
	@printf "Build\n"
	@printf "  build             Compile the ingestor binary (bin/ingestor)\n"
	@printf "  run               Run the ingestor locally\n"
	@printf "\nTest\n"
	@printf "  test              Unit tests (fast, cached)\n"
	@printf "  test-race         Unit tests + race detector\n"
	@printf "  test-integration  Integration tests only (-tags=integration)\n"
	@printf "  test-all          Unit + integration tests\n"
	@printf "  coverage          HTML coverage report (coverage.html)\n"
	@printf "\nCode quality\n"
	@printf "  lint              golangci-lint\n"
	@printf "  fmt               gofmt -w -s\n"
	@printf "  vet               go vet\n"
	@printf "\nInfrastructure\n"
	@printf "  infra-up          Start Kafka via Docker Compose\n"
	@printf "  infra-down        Stop Docker Compose\n"
	@printf "  infra-logs        Follow Kafka logs\n"
	@printf "\nOther\n"
	@printf "  proto             Regenerate protobuf files\n"
	@printf "  docker-build      Build Docker image (tagged with git version)\n"
	@printf "  docker-run        Run the Docker image\n"
	@printf "  clean             Remove bin/, coverage files\n\n"
