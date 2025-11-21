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