package metrics

import "testing"

func TestMockRecorder_IncEventsReceived(t *testing.T) {
	m := NewMock()

	m.IncEventsReceived()
	m.IncEventsReceived()
	m.IncEventsReceived()

	if m.GetEventsReceived() != 3 {
		t.Errorf("expected 3, got %d", m.GetEventsReceived())
	}
}

func TestMockRecorder_IncEventsDropped(t *testing.T) {
	m := NewMock()

	m.IncEventsDropped()
	m.IncEventsDropped()

	if m.GetEventsDropped() != 2 {
		t.Errorf("expected 2, got %d", m.GetEventsDropped())
	}
}

func TestMockRecorder_ObserveBatchFlush(t *testing.T) {
	m := NewMock()

	m.ObserveBatchFlush(0.5)
	m.ObserveBatchFlush(1.0)

	if m.GetBatchFlushCount() != 2 {
		t.Errorf("expected 2, got %d", m.GetBatchFlushCount())
	}
}

func TestMockRecorder_SetBufferSize(t *testing.T) {
	m := NewMock()

	m.SetBufferSize(100)

	if m.GetBufferSize() != 100 {
		t.Errorf("expected 100, got %d", m.GetBufferSize())
	}

	m.SetBufferSize(50)

	if m.GetBufferSize() != 50 {
		t.Errorf("expected 50, got %d", m.GetBufferSize())
	}
}

func TestMockRecorder_Reset(t *testing.T) {
	m := NewMock()

	m.IncEventsReceived()
	m.IncEventsDropped()
	m.ObserveBatchFlush(1.0)
	m.SetBufferSize(100)

	m.Reset()

	if m.GetEventsReceived() != 0 {
		t.Errorf("expected 0 after reset, got %d", m.GetEventsReceived())
	}
	if m.GetEventsDropped() != 0 {
		t.Errorf("expected 0 after reset, got %d", m.GetEventsDropped())
	}
	if m.GetBatchFlushCount() != 0 {
		t.Errorf("expected 0 after reset, got %d", m.GetBatchFlushCount())
	}
	if m.GetBufferSize() != 0 {
		t.Errorf("expected 0 after reset, got %d", m.GetBufferSize())
	}
}

func TestMockRecorder_ImplementsRecorder(t *testing.T) {
	m := NewMock()

	// Compile-time check that MockRecorder implements Recorder
	var _ Recorder = m
}
