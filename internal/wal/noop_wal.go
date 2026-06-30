package wal

import (
	"sync/atomic"

	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

type NoOpWAL struct {
	seq atomic.Uint64
}

func NewNoOpWAL() *NoOpWAL {
	return &NoOpWAL{}
}

func (n *NoOpWAL) Append(_ *pb.IngestRequest) (uint64, error) {
	return n.seq.Add(1), nil
}

func (n *NoOpWAL) Acknowledge(_ uint64) error {
	return nil
}

func (n *NoOpWAL) Recover() ([]Entry, error) {
	return nil, nil
}

func (n *NoOpWAL) Close() error {
	return nil
}
