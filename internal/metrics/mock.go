package metrics

import "sync/atomic"

type MockRecorder struct {
	eventsReceived  atomic.Int64
	eventsDropped   atomic.Int64
	batchFlushCount atomic.Int64
	bufferSize      atomic.Int64
}

func NewMock() *MockRecorder {
	return &MockRecorder{}
}

func (m *MockRecorder) IncEventsReceived() {
	m.eventsReceived.Add(1)
}

func (m *MockRecorder) IncEventsDropped() {
	m.eventsDropped.Add(1)
}

func (m *MockRecorder) ObserveBatchFlush(seconds float64) {
	m.batchFlushCount.Add(1)
}

func (m *MockRecorder) SetBufferSize(size int) {
	m.bufferSize.Store(int64(size))
}

func (m *MockRecorder) GetEventsReceived() int64  { return m.eventsReceived.Load() }
func (m *MockRecorder) GetEventsDropped() int64   { return m.eventsDropped.Load() }
func (m *MockRecorder) GetBatchFlushCount() int64 { return m.batchFlushCount.Load() }
func (m *MockRecorder) GetBufferSize() int64      { return m.bufferSize.Load() }

func (m *MockRecorder) Reset() {
	m.eventsReceived.Store(0)
	m.eventsDropped.Store(0)
	m.batchFlushCount.Store(0)
	m.bufferSize.Store(0)
}
