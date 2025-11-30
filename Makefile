.PHONY: run proto kafka-up kafka-down

run:
	go run cmd/ingestor/main.go

test:
	go test ./...

clean:
	go clean
	del /Q bin\* 2>nul || true

proto:
	protoc --go_out=. --go-grpc_out=. proto/ingestor/v1/ingestor.proto

kafka-up:
	docker-compose -f deployments/docker/docker-compose.yaml up -d

kafka-down:
	docker-compose -f deployments/docker/docker-compose.yaml down

kafka-logs:
	docker-compose -f deployments/docker/docker-compose.yaml logs -f kafka
