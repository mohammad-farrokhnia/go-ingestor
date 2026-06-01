package ingestor

import (
	"testing"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func TestNewService(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

	if svc == nil {
		t.Fatal("expected service to be created")
	}
}

func TestNewService_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(5)
	svc := NewService(buf, nil, nil)

	if svc == nil {
		t.Fatal("expected service to be created with nil recorder")
	}
}

func TestService_Push_Success(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

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
	if buf.Len() != 1 {
		t.Errorf("expected 1 event in buffer, got %d", buf.Len())
	}
}

func TestService_Push_MultipleEvents(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

	for i := 0; i < 5; i++ {
		req := &pb.IngestRequest{EventId: "test"}
		if err := svc.Push(req); err != nil {
			t.Fatalf("push %d failed: %v", i, err)
		}
	}

	if recorder.GetEventsReceived() != 5 {
		t.Errorf("expected 5 events received, got %d", recorder.GetEventsReceived())
	}
	if buf.Len() != 5 {
		t.Errorf("expected 5 events in buffer, got %d", buf.Len())
	}
}

func TestService_Push_BufferFull(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(2)
	svc := NewService(buf, recorder, nil)

	svc.Push(&pb.IngestRequest{EventId: "1"})
	svc.Push(&pb.IngestRequest{EventId: "2"})

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
	if recorder.GetEventsReceived() != 3 {
		t.Errorf("expected 3 events received, got %d", recorder.GetEventsReceived())
	}
}

func TestService_Push_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, nil, nil)

	req := &pb.IngestRequest{EventId: "test-1"}
	err := svc.Push(req)

	if err != nil {
		t.Fatalf("expected no error with nil recorder, got %v", err)
	}
	if buf.Len() != 1 {
		t.Errorf("expected 1 event in buffer, got %d", buf.Len())
	}
}

func TestService_Push_BufferFull_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(1)
	svc := NewService(buf, nil, nil)

	svc.Push(&pb.IngestRequest{EventId: "1"})
	err := svc.Push(&pb.IngestRequest{EventId: "2"})

	if err == nil {
		t.Fatal("expected error when buffer full")
	}
}
