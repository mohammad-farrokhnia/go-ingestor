package sinks

import (
	"context"
	"sync"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

// MockSink is a test implementation of Sink that captures written batches.
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
	// Make a copy to avoid mutation issues
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

// Test helpers

// SetError configures the mock to return an error on Write.
func (m *MockSink) SetError(err error) {
	m.writeErr = err
}

// GetBatches returns all batches written to the sink.
func (m *MockSink) GetBatches() [][]*pb.IngestRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.batches
}

// BatchCount returns the number of batches written.
func (m *MockSink) BatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

// TotalEvents returns the total number of events across all batches.
func (m *MockSink) TotalEvents() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, b := range m.batches {
		count += len(b)
	}
	return count
}

// IsClosed returns whether Close was called.
func (m *MockSink) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

// Reset clears all recorded batches.
func (m *MockSink) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = nil
	m.writeErr = nil
	m.closed = false
}
