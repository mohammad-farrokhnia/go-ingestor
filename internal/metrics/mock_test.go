package metrics

import "testing"

func TestMockRecorder_IncEventsReceived(t *testing.T) {
	m := NewMock()

	m.IncEventsReceived("_single")
	m.IncEventsReceived("acme")
	m.IncEventsReceived("acme")

	if m.GetEventsReceived() != 3 {
		t.Errorf("expected 3, got %d", m.GetEventsReceived())
	}
	if m.GetEventsReceivedByTenant("acme") != 2 {
		t.Errorf("expected 2 for acme, got %d", m.GetEventsReceivedByTenant("acme"))
	}
}

func TestMockRecorder_IncEventsDropped(t *testing.T) {
	m := NewMock()

	m.IncEventsDropped("acme")
	m.IncEventsDropped("acme")

	if m.GetEventsDropped() != 2 {
		t.Errorf("expected 2, got %d", m.GetEventsDropped())
	}
	if m.GetEventsDroppedByTenant("acme") != 2 {
		t.Errorf("expected 2 dropped for acme, got %d", m.GetEventsDroppedByTenant("acme"))
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

	m.IncEventsReceived("_single")
	m.IncEventsDropped("_single")
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

	var _ Recorder = m
}
