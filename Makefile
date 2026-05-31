.PHONY: run test clean proto kafka-up kafka-down kafka-logs docker-build docker-run lint test-integration

run:
	go run cmd/ingestor/main.go

test:
	go test ./...

test-integration:
	go test -tags=integration ./...

lint:
	golangci-lint run ./...

clean:
	go clean
	rm -rf bin/ dist/

proto:
	protoc --go_out=paths=source_relative:./proto --go-grpc_out=paths=source_relative:./proto \
		--proto_path=proto proto/ingestor/v1/ingestor.proto

docker-build:
	docker build -t go-ingestor:latest -f deployments/docker/Dockerfile .

docker-run:
	docker run --rm -p 50051:50051 -p 8080:8080 -v $(PWD)/configs:/app/configs:ro go-ingestor:latest

kafka-up:
	docker-compose -f deployments/docker/docker-compose.yaml up -d

kafka-down:
	docker-compose -f deployments/docker/docker-compose.yaml down

kafka-logs:
	docker-compose -f deployments/docker/docker-compose.yaml logs -f kafka
