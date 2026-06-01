FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o ingestor ./cmd/ingestor

FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata && \
    adduser -D -g '' app && \
    mkdir -p /app/data/dlq /app/data/wal /app/configs && \
    chown -R app:app /app

USER app
WORKDIR /app

COPY --from=builder /app/ingestor ./ingestor
COPY --chown=app:app configs/config.yaml.example ./configs/config.yaml

EXPOSE 50051 8080

ENTRYPOINT ["./ingestor"]
