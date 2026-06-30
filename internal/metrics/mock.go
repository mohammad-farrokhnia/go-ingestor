package metrics

import (
	"sync"
	"sync/atomic"
)

type MockRecorder struct {
	eventsReceived  atomic.Int64
	eventsDropped   atomic.Int64
	batchFlushCount atomic.Int64
	bufferSize      atomic.Int64
	workerPanics    atomic.Int64

	mu               sync.Mutex
	receivedByTenant map[string]int64
	droppedByTenant  map[string]int64
}

func NewMock() *MockRecorder {
	return &MockRecorder{
		receivedByTenant: make(map[string]int64),
		droppedByTenant:  make(map[string]int64),
	}
}

func (m *MockRecorder) IncEventsReceived(tenant string) {
	m.eventsReceived.Add(1)
	m.mu.Lock()
	m.receivedByTenant[tenant]++
	m.mu.Unlock()
}

func (m *MockRecorder) IncEventsDropped(tenant string) {
	m.eventsDropped.Add(1)
	m.mu.Lock()
	m.droppedByTenant[tenant]++
	m.mu.Unlock()
}

func (m *MockRecorder) GetEventsReceivedByTenant(tenant string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.receivedByTenant[tenant]
}

func (m *MockRecorder) GetEventsDroppedByTenant(tenant string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.droppedByTenant[tenant]
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
	m.workerPanics.Store(0)
	m.mu.Lock()
	m.receivedByTenant = make(map[string]int64)
	m.droppedByTenant = make(map[string]int64)
	m.mu.Unlock()
}

func (m *MockRecorder) IncWorkerPanics()           { m.workerPanics.Add(1) }
func (m *MockRecorder) GetWorkerPanicCount() int64 { return m.workerPanics.Load() }
