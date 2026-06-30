# ingestor

A high-throughput, buffer-based ingestion gateway written in Go. Accepts fire-and-forget events via gRPC and HTTP, buffers them in memory (or Redis), and batch-forwards to pluggable sinks with retry, circuit breaker, and dead letter queue guarantees.

## Architecture

```
  gRPC :50051 ──┐                                                    ┌── LogSink
                ├──► Service.Push() ──► Buffer ──► Worker Pool ──►──┼── KafkaSink
  HTTP :8080  ──┘        │               │            │              └── HTTPSink
                         │          [chan | redis]     │
                    (non-blocking)                     └── on failure ──► DLQ
```

- **Ingestors** — gRPC and HTTP servers accept events
- **Buffer** — Swappable: Go channel (default) or Redis-backed for multi-replica shared state
- **Workers** — Concurrent pool with configurable batch size and timeout-based flushing
- **Sinks** — Pluggable destinations, each wrapped in a circuit breaker with retry + exponential backoff
- **DLQ** — Dead Letter Queue (file-based daily JSONL or Kafka topic) captures events that exhaust retries

## Features

- Dual ingest protocols (gRPC + HTTP)
- Swappable buffer backend (channel or Redis)
- Pluggable sinks: Log, Kafka, HTTP webhook
- Per-sink circuit breaker (sony/gobreaker)
- Retry with exponential backoff (3 attempts)
- Dead Letter Queue (file or Kafka)
- Prometheus metrics with interface-based recorder
- Structured JSON logging (log/slog)
- Graceful shutdown with configurable drain timeout
- Kubernetes-ready (Deployment, Service, ConfigMap, HPA)
- GOMAXPROCS auto-tuning for containers
- CI pipeline (GitHub Actions: lint, test, build, docker)

## Getting Started

### Prerequisites

- Go 1.22+
- `protoc` compiler (only for proto regeneration)
- Docker (optional, for containerized/Kafka setup)

### Quick Start

```bash
git clone https://github.com/mohammad-farrokhnia/ingestor.git
cd ingestor
cp configs/config.yaml.example configs/config.yaml
make run
```

### Verify

```bash
curl http://localhost:8080/health          # ok
curl http://localhost:8080/ready           # ready
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt-1","source":"web","payload":"{}","timestamp":1717100000}'
# {"status":"OK"}
```

## API Reference

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/ingest` | Accept event into buffer |
| GET | `/health` | Liveness probe (always 200) |
| GET | `/ready` | Readiness probe (200 when workers running) |
| GET | `/metrics` | Prometheus metrics |

### POST /ingest

**Request body:**
```json
{
  "event_id": "uuid-string",
  "source": "service-name",
  "payload": "{\"key\":\"value\"}",
  "timestamp": 1717100000
}
```

**Responses:**

| HTTP | Status | Meaning |
|------|--------|---------|
| 202 | `OK` | Event accepted into buffer |
| 400 | `ERROR` | Missing `event_id`, invalid JSON, unknown fields, body > 1 MiB |
| 503 | `DISABLED` | Ingest disabled via config |
| 503 | `DROPPED` | Buffer full, event dropped |

### gRPC

```protobuf
service IngestorService {
  rpc Ingest (IngestRequest) returns (IngestResponse);
}
```

Port `50051` (configurable). See `proto/ingestor/v1/ingestor.proto` for full contract.

## Observability

### Prometheus Metrics

Exposed at `GET /metrics`.

| Metric | Type | Description |
|--------|------|-------------|
| `events_received_total` | Counter | Total events received |
| `events_dropped_total` | Counter | Events dropped (buffer full, invalid) |
| `batch_flush_duration_seconds` | Histogram | Batch flush latency |
| `buffer_current_size` | Gauge | Current buffered event count |

### Health & Readiness

- `GET /health` — always 200 (liveness)
- `GET /ready` — 200 when workers running, 503 during startup/shutdown

## Configuration

Copy `configs/config.yaml.example` → `configs/config.yaml`:

```yaml
server:
  grpc_port: 50051
  http_port: 8080
  ingest_enabled: true

ingestor:
  buffer_size: 1000

buffer:
  type: "channel"                # "channel" or "redis"
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    key: "ingestor:buffer"

worker:
  num_workers: 5
  batch_size: 100
  batch_timeout: "5s"

sinks:
  active: ["log"]                # log, kafka, http
  kafka:
    brokers: ["localhost:9092"]
    topic: "events"
  http:
    url: "https://webhook.example.com/events"
    timeout: "5s"

dlq:
  enabled: true
  type: "file"                   # file, kafka
  file:
    dir: "data/dlq"
  kafka:
    brokers: ["localhost:9092"]
    topic: "events-dlq"

logging:
  level: "info"                  # debug, info, warn, error
  format: "json"                 # json, text

shutdown:
  timeout: "30s"
```

Environment variables override config via `_` separator (e.g. `SINKS_KAFKA_TOPIC=events`).

## Deployment

### Docker

```bash
make docker-build
make docker-run
```

### Docker Compose (with Kafka)

```bash
make kafka-up      # Zookeeper + Kafka + Kafka UI + ingestor
make kafka-down
make kafka-logs
```

### Kubernetes

```bash
kubectl apply -f deployments/k8s/
```

Manifests in `deployments/k8s/`:

- **deployment.yaml** — 2 replicas, resource limits, liveness/readiness probes
- **service.yaml** — ClusterIP exposing gRPC (50051) and HTTP (8080)
- **configmap.yaml** — full config mounted into pods
- **hpa.yaml** — CPU-based autoscaler (70% target, 2–10 replicas)

## Scaling

Stateless replicas behind a load balancer. Buffer backend determines shared state:

| Buffer Type | Use Case | Trade-off |
|-------------|----------|-----------|
| `channel` | Single instance / independent replicas | Fastest, no deps, events lost if pod dies |
| `redis` | Shared buffer across replicas | Survives restarts, slight network latency |

When using Kafka as a sink, each replica writes independently. Kafka handles ordering via `event_id` keys and partitioning.

## Testing

```bash
make test                    # all unit tests
make lint                    # golangci-lint
go test -v ./...             # verbose
go test -cover ./...         # with coverage
make test-integration        # integration tests (build tag)
```

## Project Structure

```
cmd/ingestor/main.go          Entry point, wires all components
configs/                       Viper config loading + validation
internal/buffer/               Buffer interface (channel, redis)
internal/ingestor/             Core Service (Push, Close)
internal/server/               gRPC + HTTP servers
internal/worker/               Worker pool (batch, flush, retry)
internal/sinks/                Sink interface + implementations
internal/dlq/                  Dead Letter Queue (file, kafka, noop)
internal/metrics/              Prometheus recorder + interface
internal/logging/              slog-based structured logger
proto/ingestor/v1/             Protobuf contract + generated code
deployments/docker/            Dockerfile + docker-compose
deployments/k8s/               Kubernetes manifests
.github/workflows/ci.yaml     CI pipeline
```

## License

MIT