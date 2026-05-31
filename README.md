# go-ingestor

A high-throughput, buffer-based ingestion gateway written in Go. Designed to prioritize write-speed and availability by buffering "fire-and-forget" events and batch-forwarding them to various sinks (Kafka, Logs, HTTP).

## Architecture

```
Client ──► gRPC / HTTP ──► Buffer (channel) ──► Worker Pool ──► Sink(s)
                                                      │
                                                      └──► DLQ (on failure)
```

- **Ingestors** — gRPC and HTTP servers accept events.
- **Buffer** — Bounded Go channel for non-blocking, fire-and-forget ingestion.
- **Workers** — Concurrent worker pool drains the buffer in configurable batches.
- **Sinks** — Pluggable destinations (Kafka, HTTP webhook, stdout log). Each wrapped in a circuit breaker.
- **DLQ** — Dead Letter Queue (file or Kafka) captures events that fail all retry attempts.

## Getting Started

### Prerequisites

- Go 1.22+
- Protoc compiler (for proto regeneration only)

### Running Locally

```bash
cp configs/config.yaml.example configs/config.yaml  # edit as needed
make proto   # generate gRPC stubs (only if proto changed)
make run
```

## HTTP Ingest Endpoint

**POST** `/ingest`

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt-1", "source": "web", "payload": "{}", "timestamp": 1717100000}'
```

| Status | Code | Meaning |
|--------|------|---------|
| `OK` | 202 | Event accepted into buffer |
| `ERROR` | 400 | Missing `event_id` or invalid JSON |
| `DISABLED` | 503 | Ingest disabled via config |
| `DROPPED` | 503 | Buffer full — event dropped |

The endpoint respects the `server.ingest_enabled` config flag and body size is capped at 1 MiB.

## gRPC Ingest

The `IngestorService.Ingest` RPC accepts an `IngestRequest` (see `proto/ingestor/v1/ingestor.proto`). Same status semantics as the HTTP endpoint.

## Observability

### Prometheus Metrics

Exposed at `http://localhost:8080/metrics`.

| Metric | Type | Description |
|--------|------|-------------|
| `events_received_total` | Counter | Total events received |
| `events_dropped_total` | Counter | Events dropped (buffer full, invalid) |
| `batch_flush_duration_seconds` | Histogram | Batch flush latency |
| `buffer_current_size` | Gauge | Current buffered event count |

Built with interface-based design (`metrics.Recorder`) for easy backend swapping.

### Health & Readiness

- `GET /health` — always 200 (liveness)
- `GET /ready` — 200 when workers are running, 503 during startup/shutdown

## Configuration Reference

Copy `configs/config.yaml.example` to `configs/config.yaml`. All fields:

```yaml
server:
  grpc_port: 50051
  http_port: 8080
  ingest_enabled: true

ingestor:
  buffer_size: 1000

buffer:
  type: "channel"
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
  active: ["log"]          # log, kafka, http
  kafka:
    brokers: ["localhost:9092"]
    topic: "events"
  http:
    url: "https://webhook.example.com/events"
    timeout: "5s"

dlq:
  enabled: true
  type: "file"             # file, kafka
  file:
    dir: "data/dlq"
  kafka:
    brokers: ["localhost:9092"]
    topic: "events-dlq"

logging:
  level: "info"            # debug, info, warn, error
  format: "json"           # json, text

shutdown:
  timeout: "30s"           # max time to drain buffer before forcing exit
```

Environment variables override config via `_` separator (e.g. `SINKS_KAFKA_TOPIC=events`).

## Docker

### Build & Run

```bash
make docker-build
make docker-run
```

### Docker Compose (with Kafka)

```bash
make kafka-up     # starts Zookeeper + Kafka + Kafka UI + ingestor
make kafka-down
make kafka-logs
```

## Kubernetes

Manifests are in `deployments/k8s/`:

- **deployment.yaml** — 2 replicas, resource limits, liveness (`/health`) and readiness (`/ready`) probes
- **service.yaml** — ClusterIP exposing gRPC (50051) and HTTP (8080)
- **configmap.yaml** — full `config.yaml` mounted into pods
- **hpa.yaml** — CPU-based HorizontalPodAutoscaler (70% target, 2–10 replicas)

GOMAXPROCS is auto-tuned to container CPU limits via `go.uber.org/automaxprocs`.

```bash
kubectl apply -f deployments/k8s/
```

## Scaling

The ingestor is designed to scale horizontally as stateless replicas behind a load balancer.

| Buffer Type | Use Case | Trade-off |
|-------------|----------|----------|
| `channel` (default) | Single instance or independent replicas | Fastest, no external dependency, events lost if pod dies |
| `redis` | Shared buffer across replicas | Survives pod restarts, slight latency from network round-trip |

Set `buffer.type: redis` and configure `buffer.redis.*` to enable shared buffering.

When using **Kafka as a sink**, each replica writes independently. Kafka handles deduplication via `event_id` keys and partitioning.

## Local Kafka Development

### Start Kafka
```bash
make kafka-up