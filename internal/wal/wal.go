package wal

import (
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

// Entry represents a single WAL record with a unique sequence number.
type Entry struct {
	SeqNum uint64
	Event  *pb.IngestRequest
}

// WAL defines the Write-Ahead Log interface.
// Events are appended before processing and acknowledged after successful sink writes.
type WAL interface {
	// Append persists an event to the WAL and returns its sequence number.
	Append(event *pb.IngestRequest) (uint64, error)

	// Acknowledge marks a sequence number as successfully processed.
	Acknowledge(seqNum uint64) error

	// Recover returns all unacknowledged entries for replay on startup.
	Recover() ([]Entry, error)

	// Close flushes and closes the WAL.
	Close() error
}
