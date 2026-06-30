package wal

import (
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

type Entry struct {
	SeqNum uint64
	Event  *pb.IngestRequest
}

type WAL interface {
	Append(event *pb.IngestRequest) (uint64, error)

	Acknowledge(seqNum uint64) error

	Recover() ([]Entry, error)

	Close() error
}
