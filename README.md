# go-ingestor
A high-throughput, buffer-based ingestion gateway in Go.
A high-throughput, buffer-based ingestion gateway written in Go. Designed to prioritize write-speed and availability by buffering "fire-and-forget" events and batch-forwarding them to various sinks (Kafka, Logs, etc.).
## Architecture
- Ingestors: gRPC and HTTP servers accept events.
- Buffer: A high-performance channel buffers incoming events.
- Workers: A concurrent worker pool drains the buffer.
- Sinks: Pluggable destinations (Kafka, Stdout) receive batched events.
## Getting Started

### Prerequisites

- Go 1.22+
- Protoc compiler

### Running Locally
- Generate Proto: make proto
- Run: make run

## Observability

### Prometheus Metrics

The ingestor exposes metrics at `http://localhost:8080/metrics` (configurable via `server.http_port` in `config.yaml`).

**Available metrics:**
- `events_received_total` – Total events received by the ingestor
- `events_dropped_total` – Events dropped (buffer full, invalid requests)
- `batch_flush_duration_seconds` – Histogram of batch flush operation durations
- `buffer_current_size` – Current number of events buffered in memory

**Architecture:** Built with interface-based design (`metrics.Recorder`) allowing easy swapping of metrics backends (Prometheus, OpenTelemetry, etc.).

Built with [Prometheus Go client](https://github.com/prometheus/client_golang).