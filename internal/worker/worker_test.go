package worker

import (
	"context"
	"testing"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func TestFlush_EmptyBatch(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	sinkList := []sinks.Sink{mockSink}

	flush(0, []*pb.IngestRequest{}, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	if mockSink.BatchCount() != 1 {
		t.Errorf("expected 1 batch (even if empty), got %d", mockSink.BatchCount())
	}
	if recorder.GetBatchFlushCount() != 1 {
		t.Errorf("expected 1 flush recorded, got %d", recorder.GetBatchFlushCount())
	}
}

func TestFlush_SingleEvent(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	sinkList := []sinks.Sink{mockSink}

	batch := []*pb.IngestRequest{
		{EventId: "event-1", Source: "test"},
	}

	flush(0, batch, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	if mockSink.TotalEvents() != 1 {
		t.Errorf("expected 1 event, got %d", mockSink.TotalEvents())
	}
	if recorder.GetBatchFlushCount() != 1 {
		t.Errorf("expected 1 flush recorded, got %d", recorder.GetBatchFlushCount())
	}
}

func TestFlush_MultipleSinks(t *testing.T) {
	mockSink1 := sinks.NewMockSink()
	mockSink2 := sinks.NewMockSink()
	recorder := metrics.NewMock()
	sinkList := []sinks.Sink{mockSink1, mockSink2}

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
		{EventId: "event-2"},
	}

	flush(0, batch, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	if mockSink1.TotalEvents() != 2 {
		t.Errorf("sink1: expected 2 events, got %d", mockSink1.TotalEvents())
	}
	if mockSink2.TotalEvents() != 2 {
		t.Errorf("sink2: expected 2 events, got %d", mockSink2.TotalEvents())
	}
	if recorder.GetBatchFlushCount() != 1 {
		t.Errorf("expected 1 flush recorded, got %d", recorder.GetBatchFlushCount())
	}
}

func TestFlush_NilRecorder(t *testing.T) {
	mockSink := sinks.NewMockSink()
	sinkList := []sinks.Sink{mockSink}

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	flush(0, batch, sinkList, nil, dlq.NewNoOpDLQ(), nil, nil)

	if mockSink.TotalEvents() != 1 {
		t.Errorf("expected 1 event, got %d", mockSink.TotalEvents())
	}
}

func TestFlush_SinkError(t *testing.T) {
	mockSink := sinks.NewMockSink()
	mockSink.SetError(errTestSinkError)
	recorder := metrics.NewMock()
	sinkList := []sinks.Sink{mockSink}

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	flush(0, batch, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	if recorder.GetBatchFlushCount() != 1 {
		t.Errorf("expected 1 flush recorded, got %d", recorder.GetBatchFlushCount())
	}
}

var errTestSinkError = &testError{msg: "test sink error"}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestWorker_BatchSizeFlush(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	buffer := make(chan *pb.IngestRequest, 100)
	sinkList := []sinks.Sink{mockSink}

	ctx := context.Background()
	go runWorker(ctx, 0, buffer, 3, 10*time.Second, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	buffer <- &pb.IngestRequest{EventId: "1"}
	buffer <- &pb.IngestRequest{EventId: "2"}
	buffer <- &pb.IngestRequest{EventId: "3"}

	time.Sleep(100 * time.Millisecond)

	if mockSink.BatchCount() != 1 {
		t.Errorf("expected 1 batch, got %d", mockSink.BatchCount())
	}
	if mockSink.TotalEvents() != 3 {
		t.Errorf("expected 3 events, got %d", mockSink.TotalEvents())
	}
}

func TestWorker_TimeoutFlush(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	buffer := make(chan *pb.IngestRequest, 100)
	sinkList := []sinks.Sink{mockSink}

	ctx := context.Background()
	go runWorker(ctx, 0, buffer, 100, 50*time.Millisecond, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	buffer <- &pb.IngestRequest{EventId: "1"}
	buffer <- &pb.IngestRequest{EventId: "2"}

	time.Sleep(150 * time.Millisecond)

	if mockSink.BatchCount() < 1 {
		t.Errorf("expected at least 1 batch from timeout, got %d", mockSink.BatchCount())
	}
	if mockSink.TotalEvents() != 2 {
		t.Errorf("expected 2 events, got %d", mockSink.TotalEvents())
	}
}

func TestStart_DrainsBufferOnClose(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	buffer := make(chan *pb.IngestRequest, 100)
	sinkList := []sinks.Sink{mockSink}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := Start(ctx, 3, buffer, 1000, "10s", sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	const total = 50
	for i := 0; i < total; i++ {
		buffer <- &pb.IngestRequest{EventId: "event"}
	}

	close(buffer)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not drain and exit after buffer close")
	}

	if mockSink.TotalEvents() != total {
		t.Errorf("expected all %d events drained, got %d", total, mockSink.TotalEvents())
	}
}

func TestWorker_MultipleBatches(t *testing.T) {
	mockSink := sinks.NewMockSink()
	recorder := metrics.NewMock()
	buffer := make(chan *pb.IngestRequest, 100)
	sinkList := []sinks.Sink{mockSink}

	ctx := context.Background()
	go runWorker(ctx, 0, buffer, 2, 10*time.Second, sinkList, recorder, dlq.NewNoOpDLQ(), nil, nil)

	for i := 0; i < 5; i++ {
		buffer <- &pb.IngestRequest{EventId: "event"}
	}

	time.Sleep(100 * time.Millisecond)

	if mockSink.BatchCount() < 2 {
		t.Errorf("expected at least 2 batches, got %d", mockSink.BatchCount())
	}
	if mockSink.TotalEvents() < 4 {
		t.Errorf("expected at least 4 events flushed, got %d", mockSink.TotalEvents())
	}
}
