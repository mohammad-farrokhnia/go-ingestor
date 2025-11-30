package ingestor

import (
	"testing"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func TestNewService(t *testing.T) {
	recorder := metrics.NewMock()
	svc := NewService(10, recorder)

	if svc == nil {
		t.Fatal("expected service to be created")
	}
	if cap(svc.Buffer) != 10 {
		t.Errorf("expected buffer capacity 10, got %d", cap(svc.Buffer))
	}
}

func TestNewService_NilRecorder(t *testing.T) {
	svc := NewService(5, nil)

	if svc == nil {
		t.Fatal("expected service to be created with nil recorder")
	}
}

func TestService_Push_Success(t *testing.T) {
	recorder := metrics.NewMock()
	svc := NewService(10, recorder)

	req := &pb.IngestRequest{
		EventId: "test-1",
		Source:  "test",
		Payload: `{"key": "value"}`,
	}

	err := svc.Push(req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if recorder.GetEventsReceived() != 1 {
		t.Errorf("expected 1 event received, got %d", recorder.GetEventsReceived())
	}
	if len(svc.Buffer) != 1 {
		t.Errorf("expected 1 event in buffer, got %d", len(svc.Buffer))
	}
}

func TestService_Push_MultipleEvents(t *testing.T) {
	recorder := metrics.NewMock()
	svc := NewService(10, recorder)

	for i := 0; i < 5; i++ {
		req := &pb.IngestRequest{EventId: "test"}
		if err := svc.Push(req); err != nil {
			t.Fatalf("push %d failed: %v", i, err)
		}
	}

	if recorder.GetEventsReceived() != 5 {
		t.Errorf("expected 5 events received, got %d", recorder.GetEventsReceived())
	}
	if len(svc.Buffer) != 5 {
		t.Errorf("expected 5 events in buffer, got %d", len(svc.Buffer))
	}
}

func TestService_Push_BufferFull(t *testing.T) {
	recorder := metrics.NewMock()
	svc := NewService(2, recorder) // Small buffer

	// Fill buffer
	svc.Push(&pb.IngestRequest{EventId: "1"})
	svc.Push(&pb.IngestRequest{EventId: "2"})

	// This should fail
	err := svc.Push(&pb.IngestRequest{EventId: "3"})

	if err == nil {
		t.Fatal("expected error when buffer full")
	}
	if err.Error() != "buffer is full" {
		t.Errorf("expected 'buffer is full' error, got %v", err)
	}
	if recorder.GetEventsDropped() != 1 {
		t.Errorf("expected 1 event dropped, got %d", recorder.GetEventsDropped())
	}
	// Events received should still be incremented (we received it, then dropped)
	if recorder.GetEventsReceived() != 3 {
		t.Errorf("expected 3 events received, got %d", recorder.GetEventsReceived())
	}
}

func TestService_Push_NilRecorder(t *testing.T) {
	svc := NewService(10, nil)

	req := &pb.IngestRequest{EventId: "test-1"}
	err := svc.Push(req)

	if err != nil {
		t.Fatalf("expected no error with nil recorder, got %v", err)
	}
	if len(svc.Buffer) != 1 {
		t.Errorf("expected 1 event in buffer, got %d", len(svc.Buffer))
	}
}

func TestService_Push_BufferFull_NilRecorder(t *testing.T) {
	svc := NewService(1, nil)

	svc.Push(&pb.IngestRequest{EventId: "1"})
	err := svc.Push(&pb.IngestRequest{EventId: "2"})

	if err == nil {
		t.Fatal("expected error when buffer full")
	}
}
