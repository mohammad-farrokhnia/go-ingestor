# go-ingestor — Project Blueprint (Updated)

A high-throughput, buffer-based ingestion gateway written in Go. Accepts fire-and-forget events via gRPC and HTTP, buffers them in memory (or Redis), and batch-forwards to pluggable sinks with retry, circuit breaker, and dead letter queue guarantees.

*Last updated: 2026-06-07 — Added i18n/multi-lang system, improved error classification design, go-ledger cross-project analysis.*

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Tech Stack](#tech-stack)
3. [Goals](#goals)
4. [Architecture](#architecture)
5. [What Has Been Implemented](#what-has-been-implemented)
6. [What Needs to Be Implemented (v1.0.0)](#what-needs-to-be-implemented-v100)
7. [i18n / Multi-Language Design](#i18n--multi-language-design)
8. [Error Classification Design](#error-classification-design)
9. [Advanced Features (Post v1.0.0)](#advanced-features-post-v100)
10. [Configuration Reference](#configuration-reference)
11. [How to Run](#how-to-run)
12. [How to Test](#how-to-test)
13. [API Reference](#api-reference)
14. [Development Plan](#development-plan)
15. [Design Decisions (ADRs)](#design-decisions-adrs)
16. [Requirements & Performance Targets](#requirements--performance-targets)
17. [Versioning Strategy](#versioning-strategy)
18. [Production Issues & Vulnerabilities](#production-issues--vulnerabilities)

---

## Project Structure

```
go-ingestor/
├── cmd/ingestor/
│   └── main.go                     # Entry point: wires all components, manages lifecycle
├── configs/
│   ├── config.go                   # Viper-based config loading
│   ├── definitions.go              # All config struct types
│   ├── validate.go                 # Fail-fast validation
│   └── config.yaml.example         # Template config file
├── internal/
│   ├── apperr/                     # NEW: Typed error system
│   │   ├── error.go                # AppError interface (transient/permanent)
│   │   └── codes.go                # All error code constants
│   ├── i18n/                       # NEW: Multi-language support
│   │   ├── i18n.go                 # Translate(), DetectLang(), Lang type
│   │   ├── codes.go                # MessageCode typed constants
│   │   ├── en.go                   # English translations
│   │   └── fa.go                   # Persian (Farsi) translations
│   ├── response/                   # NEW: Unified HTTP response helpers
│   │   └── response.go             # Meta struct, OK/Error/IngestResponse helpers
│   ├── buffer/
│   │   ├── buffer.go               # Buffer interface + factory
│   │   ├── channel.go              # In-process Go channel buffer
│   │   ├── hybrid.go               # Memory + disk overflow buffer
│   │   └── redis.go                # Redis-backed shared buffer (LPUSH/BRPOP)
│   ├── ingestor/
│   │   ├── service.go              # Core Service: Push(), Close(), Buf()
│   │   └── service_test.go
│   ├── server/
│   │   ├── grpc.go                 # gRPC IngestorService implementation
│   │   ├── http.go                 # HTTP server: /ingest, /health, /ready, /metrics
│   │   └── http_test.go
│   ├── worker/
│   │   ├── worker.go               # Worker pool: batching, flush, retry, DLQ push
│   │   └── worker_test.go
│   ├── sinks/
│   │   ├── definitions.go          # Sink interface, config types
│   │   ├── sink.go                 # Factory: BuildMultiSinks, BuildSink
│   │   ├── log-sink.go             # Stdout logging sink
│   │   ├── kafka-sink.go           # Kafka producer sink
│   │   ├── http-sink.go            # HTTP webhook sink
│   │   ├── circuit-breaker.go      # gobreaker wrapper per sink
│   │   ├── mock_sink.go            # Test mock
│   │   └── *_test.go
│   ├── dlq/
│   │   ├── definitions.go          # DeadLetterQueue interface, structs
│   │   ├── dlq.go                  # Factory: NewDLQ
│   │   ├── dlq-file.go             # File-based DLQ (daily JSONL rotation)
│   │   ├── kafka-dlq.go            # Kafka-based DLQ
│   │   └── noop-dlq.go             # No-op (DLQ disabled)
│   ├── metrics/
│   │   ├── recorder.go             # Recorder interface
│   │   ├── prometheus.go           # Prometheus counters/histograms/gauges
│   │   ├── metrics.go              # Constructor
│   │   ├── mock.go                 # Test mock
│   │   └── mock_test.go
│   ├── wal/
│   │   ├── wal.go                  # WAL interface
│   │   ├── file_wal.go             # File-based WAL with checkpointing
│   │   ├── noop_wal.go             # No-op WAL
│   │   ├── tracker.go              # SeqTracker: event_id → seq_num
│   │   └── file_wal_test.go
│   ├── integration/                # Integration tests (build tag: integration)
│   │   └── integration_test.go
│   └── logging/
│       └── logging.go              # slog-based structured logger init
├── proto/ingestor/v1/
│   ├── ingestor.proto              # gRPC contract
│   ├── ingestor.pb.go              # Generated
│   └── ingestor_grpc.pb.go         # Generated
├── deployments/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── docker-compose.yaml
│   └── k8s/
│       ├── deployment.yaml
│       ├── service.yaml
│       ├── configmap.yaml
│       └── hpa.yaml
├── .github/workflows/
│   └── ci.yaml
├── info/
│   └── ingestor-plan.md            # This file
├── Makefile
├── go.mod / go.sum
└── README.md
```

---

## Tech Stack

| Category | Technology | Package/Version |
|----------|-----------|-----------------)|
| Language | Go 1.24 | — |
| RPC | gRPC + Protobuf | `google.golang.org/grpc` v1.77 |
| HTTP | stdlib `net/http` | — |
| Configuration | Viper | `github.com/spf13/viper` v1.21 |
| Logging | log/slog (structured) | stdlib |
| Metrics | Prometheus | `github.com/prometheus/client_golang` v1.23 |
| Kafka | segmentio/kafka-go | `github.com/segmentio/kafka-go` v0.4.49 |
| Circuit Breaker | gobreaker | `github.com/sony/gobreaker/v2` v2.3 |
| Redis (buffer) | go-redis | `github.com/redis/go-redis/v9` v9.20 |
| GOMAXPROCS | automaxprocs | `go.uber.org/automaxprocs` v1.6 |
| Containers | Docker + Alpine | Multi-stage build |
| Orchestration | Kubernetes | Deployment, Service, ConfigMap, HPA |
| CI | GitHub Actions | lint, test, build, docker |

---

## Goals

1. **High throughput** — Accept 10,000+ events/sec per node with sub-millisecond ingestion latency.
2. **Fire-and-forget** — Clients get instant acknowledgment; processing is async.
3. **Pluggable sinks** — Write to Log, Kafka, HTTP, or any future sink via interface.
4. **Resilience** — Retry with exponential backoff, circuit breaker per sink, DLQ for failed events.
5. **Horizontal scaling** — Stateless replicas behind a load balancer; Redis buffer for shared state.
6. **Observability** — Prometheus metrics, structured JSON logging, health/readiness probes.
7. **Graceful shutdown** — Stop accepting, drain buffer, flush workers, close sinks/DLQ within configurable timeout.
8. **Cloud-native** — Docker, Kubernetes, HPA, auto-tuned GOMAXPROCS.
9. **Multi-language responses** — Client-facing messages in EN/FA (extensible), detected via `Accept-Language` header.

---

## Architecture

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                        go-ingestor                          │
                    ├─────────────────────────────────────────────────────────────┤
                    │                                                             │
  gRPC :50051 ─────┤──► Ingestor Service ──► Buffer ──► Worker Pool ──► Sink(s)  │
                    │         │                 │              │              │    │
  HTTP :8080  ─────┤──► /ingest (POST)         │              │              │    │
                    │    /admin/dlq/replay      │         [batch+flush]       │    │
                    │                           │              │              │    │
                    │   [channel|hybrid|redis]  │         [classify err]      │    │
                    │                           │         [panic recover]     │    │
                    │                           │              └── on fail ──►DLQ  │
                    │                                                             │
                    │   /health  /ready  /metrics                                 │
                    │   i18n: Accept-Language → EN/FA responses                   │
                    └─────────────────────────────────────────────────────────────┘
```

---

## What Has Been Implemented

### Core Pipeline
- [x] Dual ingest: gRPC (`IngestorService.Ingest`) and HTTP (`POST /ingest`)
- [x] Bounded in-memory channel buffer with configurable size
- [x] Swappable buffer interface (channel default, Redis for shared state)
- [x] Concurrent worker pool with configurable count, batch size, batch timeout
- [x] Batch flush on size threshold OR timeout (whichever first)
- [x] WAL (Write-Ahead Log) — file-based with checkpointing and crash recovery
- [x] Hybrid buffer (memory + disk overflow)
- [x] Event acknowledgment via WAL after successful sink write

### Sinks
- [x] `LogSink` — stdout structured log
- [x] `KafkaSink` — writes JSON to Kafka topic via segmentio/kafka-go
- [x] `HTTPSink` — POST JSON batch to webhook URL
- [x] Circuit breaker per sink (gobreaker)
- [x] Retry with exponential backoff (3 attempts, quadratic backoff)

### Dead Letter Queue
- [x] `FileDLQ` — daily-rotated JSONL files
- [x] `KafkaDLQ` — writes to Kafka DLQ topic
- [x] `NoOpDLQ` — when DLQ disabled

### Observability
- [x] Prometheus metrics: `events_received_total`, `events_dropped_total`, `batch_flush_duration_seconds`, `buffer_current_size`
- [x] Interface-based `metrics.Recorder`
- [x] Structured JSON logging via `log/slog`
- [x] `/health`, `/ready`, `/metrics` endpoints

### Configuration & Validation
- [x] Viper-based loading (YAML + env var override)
- [x] Fail-fast validation

### Shutdown & Lifecycle
- [x] Signal handling (SIGINT, SIGTERM)
- [x] Full graceful shutdown sequence with configurable timeout

### Deployment
- [x] Multi-stage Dockerfile
- [x] Docker Compose with Kafka + Zookeeper + Kafka UI
- [x] Kubernetes manifests (Deployment, Service, ConfigMap, HPA)
- [x] GitHub Actions CI

### Testing
- [x] Unit tests for: config validation, ingestor service, HTTP handlers, worker flush/retry, sinks, metrics mock

---

## What Needs to Be Implemented (v1.0.0)

| Item | Priority | Description |
|------|----------|-------------|
| Integration tests | P0 | End-to-end: gRPC → buffer → sink, HTTP ingest path, WAL recovery |
| Error classification | P1 | Transient vs permanent; skip retries for permanent errors |
| Panic recovery | P1 | Recover panics in worker goroutines; restart worker |
| DLQ replay | P1 | `POST /admin/dlq/replay` endpoint to reprocess DLQ entries |
| **i18n / multi-lang** | P1 | EN/FA messages via `Accept-Language`; unified response envelope |

### v1.0.0 Success Criteria
```
events_lost_total = 0  (always)
events_received_total = events_written_total + events_in_dlq
```

---

## i18n / Multi-Language Design

### Why i18n here (not just in a REST API)?

go-ingestor has two client-facing surfaces:
- **HTTP**: `/ingest` responses have status strings (`"Accepted"`, `"DROPPED"`, `"DISABLED"`, `"ERROR"`)
- **gRPC**: `IngestResponse.error` field carries a message string
- **Admin endpoints** (upcoming): DLQ replay, config query, health details

All of these should speak the client's language. This is especially relevant since the project is designed for a Persian-language market.

### Design (adapted and improved from the library project)

The library project has a solid i18n architecture, but it has one friction point: three files must stay in sync (error codes, i18n message codes, and the mapping between them). We simplify this by **unifying the error code and message code** — the error code IS the i18n key.

**`internal/i18n/i18n.go`**
```go
package i18n

import "strings"

type Lang string

const (
    LangEN Lang = "en"
    LangFA Lang = "fa"
)

var supported = map[Lang]map[MessageCode]string{
    LangEN: enMessages,
    LangFA: faMessages,
}

// Translate returns the message for the given code in the detected language.
// Falls back to English if the language or code is not found.
func Translate(lang Lang, code MessageCode) string {
    if msgs, ok := supported[lang]; ok {
        if msg, ok := msgs[code]; ok {
            return msg
        }
    }
    if msg, ok := supported[LangEN][code]; ok {
        return msg
    }
    return string(code)
}

// DetectLang parses the Accept-Language header (handles q-values).
func DetectLang(header string) Lang {
    if header == "" {
        return LangEN
    }
    for _, part := range strings.Split(header, ",") {
        tag := strings.Split(strings.TrimSpace(part), ";")[0]
        primary := Lang(strings.ToLower(strings.Split(tag, "-")[0]))
        if _, ok := supported[primary]; ok {
            return primary
        }
    }
    return LangEN
}
```

**`internal/i18n/codes.go`** — all message codes
```go
type MessageCode string

const (
    // Ingest statuses (HTTP + gRPC)
    MsgAccepted    MessageCode = "ACCEPTED"
    MsgDropped     MessageCode = "DROPPED"
    MsgDisabled    MessageCode = "DISABLED"
    MsgInvalidReq  MessageCode = "INVALID_REQUEST"

    // Health / system
    MsgHealthOK    MessageCode = "HEALTH_OK"
    MsgNotReady    MessageCode = "NOT_READY"

    // Admin
    MsgDLQReplayStarted MessageCode = "DLQ_REPLAY_STARTED"
    MsgDLQEmpty         MessageCode = "DLQ_EMPTY"
    MsgDLQReplayFailed  MessageCode = "DLQ_REPLAY_FAILED"

    // Error classification (for logging/DLQ metadata)
    MsgTransientError   MessageCode = "TRANSIENT_ERROR"
    MsgPermanentError   MessageCode = "PERMANENT_ERROR"
)
```

**`internal/i18n/en.go`** — English
```go
var enMessages = map[MessageCode]string{
    MsgAccepted:         "Event accepted.",
    MsgDropped:          "Buffer full. Event dropped.",
    MsgDisabled:         "Ingest is currently disabled.",
    MsgInvalidReq:       "Invalid request.",
    MsgHealthOK:         "Service is healthy.",
    MsgNotReady:         "Service is not ready.",
    MsgDLQReplayStarted: "DLQ replay started.",
    MsgDLQEmpty:         "DLQ is empty. Nothing to replay.",
    MsgDLQReplayFailed:  "DLQ replay failed.",
    MsgTransientError:   "Transient error. Will retry.",
    MsgPermanentError:   "Permanent error. Sent to DLQ.",
}
```

**`internal/i18n/fa.go`** — Persian (Farsi)
```go
//nolint:staticcheck // U+200C (ZWNJ) is intentional Persian typography
var faMessages = map[MessageCode]string{
    MsgAccepted:         "رویداد پذیرفته شد.",
    MsgDropped:          "بافر پر است. رویداد رها شد.",
    MsgDisabled:         "دریافت رویداد در حال حاضر غیرفعال است.",
    MsgInvalidReq:       "درخواست نامعتبر.",
    MsgHealthOK:         "سرویس سالم است.",
    MsgNotReady:         "سرویس آماده نیست.",
    MsgDLQReplayStarted: "بازپخش DLQ شروع شد.",
    MsgDLQEmpty:         "DLQ خالی است. چیزی برای بازپخش وجود ندارد.",
    MsgDLQReplayFailed:  "بازپخش DLQ ناموفق بود.",
    MsgTransientError:   "خطای موقتی. تلاش مجدد خواهد شد.",
    MsgPermanentError:   "خطای دائمی. به DLQ ارسال شد.",
}
```

### Unified HTTP Response Envelope

**`internal/response/response.go`**
```go
type Meta struct {
    RequestID   string `json:"requestId"`
    Timestamp   string `json:"timestamp"`
    MessageCode string `json:"messageCode"`
    Message     string `json:"message"`
    Lang        string `json:"lang"`    // "en" | "fa" — client knows direction
}

type IngestResponse struct {
    Status string `json:"status"` // "Accepted" | "DROPPED" | "DISABLED" | "ERROR"
    Meta   Meta   `json:"meta"`
}
```

HTTP handler usage:
```go
lang := i18n.DetectLang(r.Header.Get("Accept-Language"))
response.WriteIngest(w, http.StatusAccepted, "Accepted", i18n.MsgAccepted, lang)
```

### Why NOT use a heavy i18n library (go-i18n, etc.)?

The message set is small (< 20 codes) and static. A map-of-maps is zero dependencies, zero reflection, and compiles to a few KB. Add a library only if you need plurals or interpolation.

---

## Error Classification Design

### The Problem

Right now `writeWithRetry` retries every error identically — 3 attempts with quadratic backoff. This is wrong for permanent errors:

- **Transient** (retry makes sense): network timeout, HTTP 503, Kafka leader election, connection reset
- **Permanent** (retry is useless): HTTP 400 Bad Request (bad payload format), HTTP 404 (wrong endpoint), serialization failure, auth rejection

Retrying a 400 wastes time and delays moving the event to DLQ where it belongs.

### Design (adapted from library project's `ErrorType`)

**`internal/apperr/error.go`**
```go
package apperr

type ErrorClass int

const (
    Transient ErrorClass = iota  // retry makes sense
    Permanent                    // skip retries, go straight to DLQ
)

type SinkError struct {
    SinkName string
    Class    ErrorClass
    Cause    error
}

func (e *SinkError) Error() string { return e.Cause.Error() }
func (e *SinkError) Unwrap() error { return e.Cause }

func IsTransient(err error) bool {
    var se *SinkError
    if errors.As(err, &se) {
        return se.Class == Transient
    }
    return true // unknown errors are treated as transient (safe default)
}

func IsPermanent(err error) bool { return !IsTransient(err) }
```

**`internal/apperr/codes.go`** — HTTP status classification table
```go
// ClassifyHTTPStatus returns the error class for an HTTP status code.
// Used by HTTPSink to wrap errors before returning them.
func ClassifyHTTPStatus(status int) ErrorClass {
    switch {
    case status >= 500:
        return Transient  // 5xx: server-side, likely recoverable
    case status == 429:
        return Transient  // too many requests: back off and retry
    case status >= 400:
        return Permanent  // 4xx: client error, retrying won't help
    default:
        return Transient
    }
}
```

**Worker change** — skip retries for permanent errors:
```go
func writeWithRetry(ctx context.Context, sink sinks.Sink, batch []*pb.IngestRequest) error {
    var lastErr error
    for attempt := 1; attempt <= maxRetries; attempt++ {
        err := sink.Write(ctx, batch)
        if err == nil {
            return nil
        }
        lastErr = err

        // Don't retry permanent errors — go straight to DLQ
        if apperr.IsPermanent(err) {
            return err
        }

        if attempt < maxRetries {
            backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
            slog.Warn("Transient sink error, retrying",
                "sink", sink.Name(), "attempt", attempt, "backoff", backoff, "err", err)
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(backoff):
            }
        }
    }
    return lastErr
}
```

---

## Advanced Features (Post v1.0.0)

### v1.1 — Multi-Tenancy
- `tenant_id` field in IngestRequest proto
- Per-tenant buffer/rate limits
- Tenant-aware sink routing (e.g., per-tenant Kafka topics)
- Tenant isolation in metrics labels

### v1.2 — Database Sinks
- PostgreSQL, MySQL, MongoDB, ClickHouse, Redis, Elasticsearch
- Connection pooling, batch inserts, configurable timeouts

### v1.3 — Batch & Compression
- Accept batched requests (`BatchIngestRequest`)
- Compress payloads (Snappy, LZ4, Zstd, Gzip)

### v2.0 — Advanced Protocol
- Client-side streaming gRPC for higher throughput
- Event schema validation (JSON Schema)
- Event enrichment and transformation pipeline
- Event deduplication and filtering

### v2.1 — Enterprise
- Admin API (pause/resume, config hot-reload, DLQ replay trigger — partial in v1.0.0)
- JWT/API-key authentication middleware
- RBAC (super_admin, tenant_admin, operator, viewer)
- Audit logging
- OpenTelemetry distributed tracing

### v3.0 — Storage Expansion
- Object storage sinks: S3, MinIO, GCS, Azure Blob, HDFS
- Helm chart for Kubernetes
- GitOps deployment (ArgoCD/Flux)
- ServiceMonitor + Grafana dashboard templates
- Performance optimization (100k+ events/sec per node)

---

## Configuration Reference

```yaml
server:
  grpc_port: 50051
  http_port: 8080
  ingest_enabled: true

ingestor:
  buffer_size: 1000

buffer:
  type: "channel"                # "channel" | "redis" | "hybrid"
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    key: "ingestor:buffer"
  hybrid:
    dir: "data/buffer"

worker:
  num_workers: 5
  batch_size: 100
  batch_timeout: "5s"

sinks:
  active: ["log"]                # log | kafka | http
  kafka:
    brokers: ["localhost:9092"]
    topic: "events"
  http:
    url: "https://webhook.example.com/events"
    timeout: "5s"

dlq:
  enabled: true
  type: "file"                   # "file" | "kafka"
  file:
    dir: "data/dlq"
  kafka:
    brokers: ["localhost:9092"]
    topic: "events-dlq"

wal:
  enabled: false
  dir: "data/wal"
  checkpoint_interval: "60s"

logging:
  level: "info"
  format: "json"

shutdown:
  timeout: "30s"

i18n:
  default_lang: "en"             # "en" | "fa"
```

---

## How to Run

### Local Development
```bash
git clone https://github.com/mohammad-farrokhnia/go-ingestor.git
cd go-ingestor
cp configs/config.yaml.example configs/config.yaml
make run
```

### With Kafka (Docker Compose)
```bash
make kafka-up
make kafka-logs
make kafka-down
```

### Kubernetes
```bash
kubectl apply -f deployments/k8s/
```

### Verify
```bash
# Health check
curl http://localhost:8080/health
curl http://localhost:8080/ready

# Ingest — English response
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "Accept-Language: en" \
  -d '{"event_id":"evt-1","source":"web","payload":"{}","timestamp":1717100000}'
# → {"status":"Accepted","meta":{"messageCode":"ACCEPTED","message":"Event accepted.","lang":"en",...}}

# Ingest — Persian response
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "Accept-Language: fa,en;q=0.9" \
  -d '{"event_id":"evt-2","source":"app","payload":"{}","timestamp":1717100001}'
# → {"status":"Accepted","meta":{"messageCode":"ACCEPTED","message":"رویداد پذیرفته شد.","lang":"fa",...}}
```

---

## How to Test

### Unit Tests
```bash
make test
go test ./...
```

### Integration Tests (build tag)
```bash
make test-integration
go test -tags=integration ./internal/integration/...
```

### Test Coverage
```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### What the Tests Cover

| Package | Tests |
|---------|-------|
| `configs` | Validation: ports, buffer, workers, sinks, DLQ, timeout |
| `internal/ingestor` | Service creation, Push success/failure, buffer full, nil recorder |
| `internal/server` | HTTP ingest: success, method not allowed, disabled, missing fields, unknown fields, buffer full, body limit, health/ready |
| `internal/worker` | Worker flush, retry with backoff, DLQ push on failure, batch by size, batch by timeout, buffer drain on close |
| `internal/sinks` | Log sink write, HTTP sink (success, errors, timeout, context cancel), sink factory, circuit breaker |
| `internal/metrics` | Mock recorder thread safety |
| `internal/wal` | Append, acknowledge, recover, checkpoint |
| `internal/buffer` | Hybrid buffer: memory fill, disk overflow, drain back, crash recovery |
| `internal/i18n` | EN/FA translation, Accept-Language detection, fallback, unknown codes |
| `internal/apperr` | Error classification, transient/permanent detection, HTTP status mapping |
| `internal/integration` | E2E: HTTP ingest → buffer → mock sink; gRPC ingest → buffer; WAL recovery |

---

## API Reference

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/ingest` | Accept event into buffer |
| POST | `/admin/dlq/replay` | Reprocess DLQ entries *(v1.0.0)* |
| GET | `/health` | Liveness probe (always 200) |
| GET | `/ready` | Readiness probe (200 when workers running) |
| GET | `/metrics` | Prometheus metrics |

#### POST /ingest Response Format (after i18n)

**202 Accepted:**
```json
{
  "status": "Accepted",
  "meta": {
    "requestId": "uuid-here",
    "timestamp": "2026-06-07T10:00:00Z",
    "messageCode": "ACCEPTED",
    "message": "Event accepted.",
    "lang": "en"
  }
}
```

**503 Dropped:**
```json
{
  "status": "DROPPED",
  "meta": {
    "requestId": "uuid-here",
    "timestamp": "2026-06-07T10:00:00Z",
    "messageCode": "DROPPED",
    "message": "بافر پر است. رویداد رها شد.",
    "lang": "fa"
  }
}
```

### gRPC Service

```protobuf
service IngestorService {
  rpc Ingest (IngestRequest) returns (IngestResponse);
}

message IngestResponse {
  string status = 1;        // "OK" | "DROPPED" | "ERROR" | "DISABLED"
  string error = 2;         // localized error message
  string message_code = 3;  // NEW: machine-readable code for client-side i18n
}
```

---

## Development Plan

### Phase 1 — Production-Ready ✅ Complete

| Step | Description | Status |
|------|-------------|--------|
| 1–13 | Core pipeline, sinks, DLQ, observability, deployment, CI | ✅ All done |

### Phase 2 — Data Reliability (v1.0.0)

| Step | Description | Status |
|------|-------------|--------|
| 14 | WAL (Write-Ahead Log) | ✅ Done |
| 15 | Hybrid buffer (memory + disk overflow) | ✅ Done |
| 16a | **Integration tests** | ❌ In progress (feature/intg-tests) |
| 16b | **Error classification** (transient/permanent) | ❌ Next after tests |
| 17 | **Panic recovery in workers** | ❌ |
| 18 | **DLQ replay mechanism** (`POST /admin/dlq/replay`) | ❌ |
| 19 | **i18n / multi-language responses** | ❌ New item |

### Phase 3 — Feature Expansion (v1.1–v1.3)

| Step | Description | Status |
|------|-------------|--------|
| 20 | Multi-tenancy | ❌ |
| 21 | Database sinks (PostgreSQL, ClickHouse, etc.) | ❌ |
| 22 | Batch ingest + compression | ❌ |
| 23 | Object storage sinks (S3, MinIO) | ❌ |

### Phase 4 — Enterprise (v2.0+)

| Step | Description | Status |
|------|-------------|--------|
| 24 | Client-side streaming gRPC | ❌ |
| 25 | Admin API + RBAC | ❌ |
| 26 | OpenTelemetry tracing | ❌ |
| 27 | TLS/mTLS + auth middleware | ❌ |
| 28 | Performance optimization (100k+ evt/s) | ❌ |
| 29 | Helm chart + GitOps | ❌ |

---

## Design Decisions (ADRs)

| # | Decision | Rationale |
|---|----------|-----------| 
| 1 | Buffered Go channel as default buffer | Zero-dependency, thread-safe, fast. Redis option for multi-replica shared state. |
| 2 | Fixed worker pool (not per-request goroutines) | Predictable resource usage, avoids goroutine explosion. |
| 3 | Sink interface (`Write`, `Name`, `Close`) | Decouples workers from destinations, easy to add new sinks, testable with mocks. |
| 4 | Prometheus pull-based metrics | Industry standard, K8s native, enables HPA on custom metrics. |
| 5 | Drop on buffer full (non-blocking) | Never blocks ingestion path; client retries. |
| 6 | Unary RPC first (streaming later) | Simpler; sufficient for 10k+ evt/s. |
| 7 | Sequential sink fan-out | Simple, predictable. |
| 8 | Hybrid buffer for production | Memory for speed + disk overflow for durability. |
| 9 | WAL for crash recovery | At-least-once delivery guarantee. |
| 10 | Deployment after reliability | Better to deploy late with reliable service than early with data loss. |
| 11 | `-ldflags` version injection | Binary carries its own version from git tag/commit. |
| 12 | Kafka-only DLQ in Kubernetes | File DLQ writes to ephemeral container disk; pod reschedule = data loss. |
| 13 | Redis Streams for distributed buffer (planned) | Atomic consumer-group semantics prevent event loss on replica crash. |
| 14 | HTTP 202 `Accepted` response | Semantically correct: buffered asynchronously, not yet processed. |
| 15 | **i18n via map-of-maps, no library** | Message set is small and static. Zero deps, zero reflection. No need for go-i18n until we need plurals or interpolation. |
| 16 | **Unified error code = i18n key (no mapping file)** | The library project has separate error codes, i18n codes, and a mapping file — all three must stay in sync. We avoid this by making the error code the message code directly. One file to update instead of three. |
| 17 | **Permanent errors skip retries** | Retrying a 400 is pointless and delays DLQ delivery. Classify at the sink level, decide at the worker level. |
| 18 | **`Accept-Language` header for lang detection** | Standard HTTP mechanism. Supports quality values. Falls back to EN. |

---

## Requirements & Performance Targets

| Requirement | Target |
|-------------|--------|
| Throughput | 10,000+ events/sec per node |
| Ingestion latency | Sub-millisecond (non-blocking channel push) |
| Scaling | Horizontal (stateless replicas behind LB) |
| Data loss | Zero tolerance (post v1.0.0 with WAL) |
| Recovery time | < 10 seconds on crash (WAL replay) |
| Shutdown drain | Complete within configurable timeout (default 30s) |

---

## Versioning Strategy

Binary carries its own version from git tag/commit via `-ldflags`:

```makefile
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"
```

| Scenario | Version value |
|----------|--------------|
| Local uncommitted changes | `v1.0.0-dirty` |
| Exact tag on commit | `v1.0.0` |
| Commits ahead of tag | `v1.0.0-3-gabcdef1` |
| Not built via Makefile | `dev` |

---

## Production Issues & Vulnerabilities

### ⚠️ Ephemeral Data Loss via File-Based DLQ in Kubernetes
**Problem:** When `dlq.type: file`, failed events are written to the container's local disk. Pod crash = permanent loss.
**Fix:** Use `dlq.type: kafka` in production. If file DLQ is needed, mount a PVC.

### ⚠️ Redis Buffer Race Condition & At-Least-Once Delivery
**Problem:** `LPUSH`/`BRPOP` Redis buffer loses events if a consumer crashes between `RPOP` and successful sink write.
**Fix (planned):** Replace with Redis Streams (`XREADGROUP`/`XACK`) for atomic consumer-group semantics.

### ⚠️ Worker Panic Kills Goroutine Silently (Not Fixed Yet)
**Problem:** A nil pointer or type assertion failure inside a worker kills that goroutine permanently. You lose 1/N of your processing capacity with no alert.
**Fix:** Wrap `runWorker` body in `defer func() { if r := recover(); r != nil { slog.Error(...); go runWorker(...) } }()`. The goroutine restarts itself.

### ⚠️ All Errors Retried Equally (Not Fixed Yet)
**Problem:** `writeWithRetry` retries HTTP 400s and serialization failures — errors that will never succeed — wasting time and delaying DLQ delivery.
**Fix:** Error classification system described above.

*Last updated: 2026-06-07*
