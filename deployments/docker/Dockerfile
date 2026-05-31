FROM golang:1.22 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/ingestor ./cmd/ingestor

FROM alpine:3.19

RUN adduser -D -g '' app
USER app
WORKDIR /app

COPY --from=builder /app/ingestor /app/ingestor
COPY --chown=app:app configs/config.yaml.example /app/configs/config.yaml

EXPOSE 50051 8000

ENTRYPOINT ["/app/ingestor"]
