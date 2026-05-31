package sinks

import (
	"context"
	"sync"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type MockSink struct {
	mu       sync.Mutex
	batches  [][]*pb.IngestRequest
	writeErr error
	closed   bool
}

func NewMockSink() *MockSink {
	return &MockSink{}
}

func (m *MockSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	batchCopy := make([]*pb.IngestRequest, len(batch))
	copy(batchCopy, batch)
	m.batches = append(m.batches, batchCopy)
	return nil
}

func (m *MockSink) Name() string {
	return "MockSink"
}

func (m *MockSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MockSink) SetError(err error) {
	m.writeErr = err
}

func (m *MockSink) GetBatches() [][]*pb.IngestRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batches
}

func (m *MockSink) BatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func (m *MockSink) TotalEvents() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, b := range m.batches {
		count += len(b)
	}
	return count
}

func (m *MockSink) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockSink) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = nil
	m.writeErr = nil
	m.closed = false
}
