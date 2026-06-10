package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/apperr"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)
type controlledSink struct {
    mu         sync.Mutex
    callCount  int
    panicUntil int
    received   [][]*pb.IngestRequest
}

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
func TestWriteWithRetry_PermanentError_CallsSinkOnce(t *testing.T) {
	mockSink := sinks.NewMockSink()
	mockSink.SetError(apperr.NewPermanent("MockSink", errors.New("400 bad request")))

	batch := []*pb.IngestRequest{{EventId: "perm-1"}}

	err := writeWithRetry(context.Background(), mockSink, batch)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !apperr.IsPermanent(err) {
		t.Errorf("expected permanent error back, got transient: %v", err)
	}
	if got := mockSink.CallCount(); got != 1 {
		t.Errorf("Write called %d times for permanent error, want exactly 1", got)
	}
}

func TestWriteWithRetry_TransientError_RetriesMaxTimes(t *testing.T) {
	mockSink := sinks.NewMockSink()
	mockSink.SetError(errors.New("connection refused"))

	batch := []*pb.IngestRequest{{EventId: "trans-1"}}

	err := writeWithRetry(context.Background(), mockSink, batch)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := mockSink.CallCount(); got != maxRetries {
		t.Errorf("Write called %d times for transient error, want %d", got, maxRetries)
	}
}



func (s *controlledSink) Write(_ context.Context, batch []*pb.IngestRequest) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.callCount++
    if s.callCount <= s.panicUntil {
        panic(fmt.Sprintf("controlled panic #%d", s.callCount))
    }
    cp := make([]*pb.IngestRequest, len(batch))
    copy(cp, batch)
    s.received = append(s.received, cp)
    return nil
}

func (s *controlledSink) TotalEvents() int {
    s.mu.Lock()
    defer s.mu.Unlock()
    n := 0
    for _, b := range s.received {
        n += len(b)
    }
    return n
}

func (s *controlledSink) Name() string  { return "ControlledSink" }
func (s *controlledSink) Close() error  { return nil }


func TestWorker_PanicRecovery_WorkerRestarts(t *testing.T) {
    origCooldown := restartCooldown
    restartCooldown = 20 * time.Millisecond
    defer func() { restartCooldown = origCooldown }()

    sink := &controlledSink{panicUntil: 1}
    recorder := metrics.NewMock()
    buf := make(chan *pb.IngestRequest, 50)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        runWorkerWithRestart(ctx, 0, buf, 5, 50*time.Millisecond, []sinks.Sink{sink}, recorder, dlq.NewNoOpDLQ(), nil, nil)
    }()

    buf <- &pb.IngestRequest{EventId: "wave1-a"}

    time.Sleep(150 * time.Millisecond)
    buf <- &pb.IngestRequest{EventId: "wave2-a"}
    buf <- &pb.IngestRequest{EventId: "wave2-b"}

    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if sink.TotalEvents() >= 2 {
            break
        }
        time.Sleep(20 * time.Millisecond)
    }

    cancel()
    wg.Wait()

    if sink.TotalEvents() < 2 {
        t.Errorf("expected at least 2 events after restart, got %d", sink.TotalEvents())
    }
    if recorder.GetWorkerPanicCount() < 1 {
        t.Errorf("expected at least 1 panic recorded, got %d", recorder.GetWorkerPanicCount())
    }
}

func TestWorker_PanicRecovery_ExceedsMaxRestarts_Stops(t *testing.T) {
    origMax := maxRestarts
    origCooldown := restartCooldown
    maxRestarts = 2
    restartCooldown = 10 * time.Millisecond
    defer func() {
        maxRestarts = origMax
        restartCooldown = origCooldown
    }()

    alwaysPanic := &controlledSink{panicUntil: 1000}
    recorder := metrics.NewMock()
    buf := make(chan *pb.IngestRequest, 20)

    ctx := context.Background()
    done := make(chan struct{})

    go func() {
        i := 0
        for {
            select {
            case buf <- &pb.IngestRequest{EventId: fmt.Sprintf("trigger-%d", i)}:
                i++
            case <-done:
                return
            }
        }
    }()

    go func() {
        runWorkerWithRestart(
            ctx, 0, buf, 1, 10*time.Millisecond,
            []sinks.Sink{alwaysPanic}, recorder,
            dlq.NewNoOpDLQ(), nil, nil,
        )
        close(done)
    }()

    select {
    case <-done:
    case <-time.After(5 * time.Second):
        t.Fatal("worker did not stop after exhausting max restarts")
    }

    if got := recorder.GetWorkerPanicCount(); int(got) != maxRestarts {
        t.Errorf("expected %d panics recorded, got %d", maxRestarts, got)
    }
}