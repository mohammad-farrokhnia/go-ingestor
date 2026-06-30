# ingestor

A high-throughput event ingestion gateway written in Go 1.24. Accepts fire-and-forget events over gRPC and HTTP, durably buffers them with an optional write-ahead log, and batch-forwards to pluggable sinks with retry, circuit breaker, and dead-letter queue guarantees.

---

## Architecture

```
  gRPC :50051 ──┐
                ├──► IngestorService.Push()
  HTTP :8080  ──┘         │         │
                      WAL.Append  Buffer.Push
                           │         │
                           │    [channel | redis | hybrid]
                           │         │
                           └────► Worker Pool (N goroutines)
                                      │
                    ┌─────────────────┼──────────────────┐
                    ▼                 ▼                   ▼
                LogSink          KafkaSink            HTTPSink
               (circuit-broken, retry + exponential backoff)
                                      │ failure
                                   DLQ (file-JSONL | Kafka)
```

### Event lifecycle

1. Client sends `POST /ingest` or gRPC `Ingest`.
2. `IngestorService.Push` writes the event to the WAL (sequence number), then to the buffer.
3. Workers drain the buffer in batches and write to every active sink.
4. On success the worker acknowledges the WAL entry (seq removed from the tracker).
5. On permanent sink failure (retries exhausted) the event goes to the DLQ.
6. On restart `replayWAL` replays any un-acknowledged WAL entries back through the service.

---

## Features

- Dual ingest protocols — gRPC (port 50051) and HTTP REST (port 8080)
- Three buffer backends — in-process channel, Redis (`BRPOP`/`LPUSH`), or hybrid (channel + disk spill)
- Pluggable sinks — Log, Kafka (`segmentio/kafka-go`), HTTP webhook
- Per-sink circuit breaker (`sony/gobreaker` v2) — opens after 5 consecutive failures
- Retry with quadratic backoff — up to 3 attempts (100 ms / 400 ms); permanent errors skip retries
- Write-ahead log (WAL) — binary file log with fsync-per-append, checkpoint compaction, crash recovery
- Dead-letter queue — daily JSONL files or Kafka topic; replayable via admin endpoint
- Multi-tenancy — optional `tenant_id` enforcement, per-tenant rate limiting (token bucket), per-tenant Kafka topic routing, and `tenant`-labelled metrics 
- i18n responses — English and Persian (Farsi) via `Accept-Language` header
- Admin endpoints — DLQ replay and stats, protected with a Bearer token
- Prometheus metrics — events received/dropped, buffer size, batch flush histogram, worker panics
- Structured JSON logging — `log/slog`
- Graceful shutdown — ordered drain (stop ingest → drain buffer → flush workers → close sinks → close WAL)
- GOMAXPROCS auto-tuning — `go.uber.org/automaxprocs` for correct CPU quota in containers

---

## Getting Started

### Prerequisites

- Go 1.24+
- `protoc` + `protoc-gen-go` (only needed to regenerate proto files)
- Docker (optional — for Kafka/Redis infrastructure)

### Quick start

```bash
git clone https://github.com/mohammad-farrokhnia/ingestor.git
cd ingestor
cp configs/config.yaml.example configs/config.yaml
make run
```

### Verify

```bash
curl http://localhost:8080/health

curl http://localhost:8080/ready

curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"event_id":"evt-1","source":"web","payload":"{}","timestamp":1717100000}'
```

Response envelope:
```json
{
  "data": { "event_id": "evt-1" },
  "meta": {
    "appName": "ingestor",
    "version": "dev",
    "requestId": "550e8400-...",
    "timestamp": "2026-06-30T10:00:00Z",
    "messageCode": "ACCEPTED",
    "message": "Event accepted.",
    "lang": "en"
  }
}
```

Error responses use the same envelope shape with an `error` key instead of `data`.

---

## API Reference

### HTTP endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/ingest` | — | Accept an event into the buffer (202) |
| `GET` | `/health` | — | Liveness probe (always 200) |
| `GET` | `/ready` | — | Readiness probe (503 during startup/shutdown) |
| `GET` | `/metrics` | — | Prometheus metrics |
| `POST` | `/admin/dlq/replay` | Bearer token | Drain all DLQ entries and re-ingest |
| `GET` | `/admin/dlq/stats` | Bearer token | Entry count and bytes per DLQ file |

### POST /ingest — request body

```json
{
  "event_id":  "uuid-or-any-string",
  "source":    "service-name",
  "payload":   "{\"key\":\"value\"}",
  "timestamp": 1717100000,
  "tenant_id": "acme"
}
```

`tenant_id` is optional unless `tenancy.enabled: true` in config.

### POST /ingest — responses

| HTTP | `messageCode` | Meaning |
|------|--------------|---------|
| 202 | `ACCEPTED` | Event buffered successfully |
| 400 | `MISSING_EVENT_ID` | `event_id` field absent |
| 400 | `MISSING_TENANT_ID` | `tenant_id` required but absent |
| 400 | `INVALID_JSON` | Malformed body or unknown fields |
| 401 | `UNAUTHORIZED` | Admin endpoint — invalid or missing token |
| 429 | `TENANT_QUOTA_EXCEEDED` | Tenant exceeded its configured rate limit |
| 503 | `DISABLED` | Ingest disabled in config or during shutdown |
| 503 | `DROPPED` | Buffer full |

### gRPC

```protobuf
service IngestorService {
  rpc Ingest (IngestRequest) returns (IngestResponse);
}

message IngestRequest {
  string event_id  = 1;
  string source    = 2;
  string payload   = 3;
  int64  timestamp = 4;
  string tenant_id = 5;
}
```

Port `50051` (configurable). See `proto/ingestor/v1/ingestor.proto`.

### i18n

Send `Accept-Language: fa` to receive all messages in Persian (Farsi). Default is English (`en`).

---

## Configuration

Copy `configs/config.yaml.example` to `configs/config.yaml`:

```yaml
server:
  grpc_port: 50051
  http_port: 8080
  ingest_enabled: true
  admin_token: ""

ingestor:
  buffer_size: 1000

buffer:
  type: "channel"           # channel | redis | hybrid
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    key: "ingestor:buffer"
  hybrid:
    dir: "data/buffer"      # disk-spill directory (hybrid mode only)

worker:
  num_workers: 4
  batch_size: 100
  batch_timeout: "5s"

sinks:
  active: ["log"]           # log | kafka | http
  kafka:
    brokers: ["localhost:9092"]
    topic: "events"
  http:
    url: "https://webhook.example.com/events"
    timeout: "5s"

dlq:
  enabled: true
  type: "file"              # file | kafka
  file:
    dir: "data/dlq"
  kafka:
    brokers: ["localhost:9092"]
    topic: "events-dlq"

wal:
  enabled: false            # set true to enable crash-recovery WAL
  dir: "data/wal"
  checkpoint_interval: "60s"

tenancy:
  enabled: false            # set true to require tenant_id on every event
  default_rate_limit: 0     # events/sec for tenants without an explicit limit; 0 = unlimited
  tenants:                  # per-tenant overrides (only consulted when enabled: true)
    - id: "acme"
      rate_limit: 5000      # events/sec; 0 = inherit default_rate_limit
      kafka_topic: "events.acme"   # routes this tenant's events to a dedicated topic
    - id: "globex"
      rate_limit: 100

logging:
  level: "info"             # debug | info | warn | error
  format: "json"

shutdown:
  timeout: "30s"
```

Environment variables override YAML via `_` separator (e.g. `SINKS_KAFKA_TOPIC=events`).

---

## Observability

### Prometheus metrics (`GET /metrics`)

| Metric | Type | Description |
|--------|------|-------------|
| `events_received_total` | Counter | Total events received (labelled by `tenant`) |
| `events_dropped_total` | Counter | Events dropped — buffer full, WAL error, or tenant quota (labelled by `tenant`) |
| `batch_flush_duration_seconds` | Histogram | Per-batch flush latency |
| `buffer_current_size` | Gauge | Current buffered event count |
| `worker_panics_total` | Counter | Recovered worker panics |

### Health probes

| Endpoint | 200 | 503 |
|----------|-----|-----|
| `/health` | always | — |
| `/ready` | workers running | startup or shutdown |

---

## Deployment

### Local (binary)

```bash
make build          # produces bin/ingestor
./bin/ingestor
```

### Infrastructure (Kafka / Redis via Docker Compose)

```bash
make infra-up       # docker compose up -d
make infra-down
make infra-logs
```

### Docker (application)

```bash
make docker-build   # builds ingestor:<git-tag>
make docker-run
```

The application binary is designed to run outside the Docker network in development for faster iteration. Use `docker-run` or your own compose file to containerize it for staging/production.

### Kubernetes

```bash
kubectl apply -f deployments/k8s/
```

Manifests:
- `deployment.yaml` — replicas, resource limits, liveness/readiness probes
- `service.yaml` — ClusterIP on gRPC (50051) and HTTP (8080)
- `configmap.yaml` — full config mounted into pods
- `hpa.yaml` — CPU-based autoscaler (70% target, 2–10 replicas)

---

## Scaling

Stateless API tier. Buffer backend determines shared state across replicas:

| Buffer | Use case | Trade-off |
|--------|----------|-----------|
| `channel` | Single instance or independent replicas | Fastest; events lost if pod dies without WAL |
| `redis` | Shared buffer across replicas | Survives restarts; slight network latency |
| `hybrid` | Single instance with disk spill | Absorbs traffic spikes; no external dep |

Enable `wal.enabled: true` for at-least-once delivery — WAL entries are replayed on restart regardless of buffer type.

---

## Testing

```bash
make test              # unit test
make test-race         # unit tests + race detector
make test-integration  # end-to-end tests (-tags=integration)
make test-all          # unit + integration
make lint              # golangci-lint (0 issues required)
make coverage          # HTML coverage report → coverage.html
```

---

## Project Structure

```
cmd/ingestor/
  main.go             Signal handling, setup → run → shutdown
  app.go              application struct, component wiring
  shutdown.go         Ordered graceful-shutdown sequence

configs/
  definitions.go      All config structs and type constants
  config.go           Viper loader
  validate.go         Per-section validation + applyDefaults

internal/
  ingestor/           Core Service (Push, WAL + buffer hand-off)
  buffer/             Buffer interface: channel, redis, hybrid
  worker/             Worker pool: batch, flush, retry, panic recovery
  sinks/              Sink interface: log, kafka, http (+ circuit breaker)
  dlq/                Dead-letter queue: file-JSONL, kafka, noop
  wal/                Write-ahead log: file-based binary log + NoOpWAL
  server/             gRPC + HTTP servers, admin endpoints
  metrics/            Prometheus recorder interface + implementation
  response/           Uniform JSON envelope (data/meta, error/meta)
  i18n/               Message codes + EN/FA translations
  apperr/             Typed error codes (permanent vs transient)

proto/ingestor/v1/    Protobuf contract + generated Go code
deployments/
  docker/             Dockerfile + docker-compose.yaml
  k8s/                Kubernetes manifests
notes/                Development plans and architecture docs
```

---

## License

MIT
