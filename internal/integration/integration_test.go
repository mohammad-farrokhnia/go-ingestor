package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/wal"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func waitFor(t *testing.T, timeout, interval time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

func getFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("getFreePort: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	errClose := lis.Close()
	if errClose != nil {
		t.Fatalf("getFreePort on closing listener: %v", err)
	}
	return port
}

type capturingDLQ struct {
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	Event    *pb.IngestRequest
	SinkName string
	Err      string
}

func (d *capturingDLQ) Push(_ context.Context, event *pb.IngestRequest, sinkName string, err error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.entries = append(d.entries, capturedEntry{
		Event:    event,
		SinkName: sinkName,
		Err:      err.Error(),
	})
	return nil
}

func (d *capturingDLQ) Close() error { return nil }
func (d *capturingDLQ) Name() string { return "capturingDLQ" }

func (d *capturingDLQ) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}

func (d *capturingDLQ) EventIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := make([]string, len(d.entries))
	for i, e := range d.entries {
		ids[i] = e.Event.EventId
	}
	return ids
}

var _ dlq.DeadLetterQueue = (*capturingDLQ)(nil)

type pipelineOpts struct {
	bufSize      int
	numWorkers   int
	batchSize    int
	batchTimeout string
	walDir       string
}

func defaultOpts() pipelineOpts {
	return pipelineOpts{
		bufSize:      100,
		numWorkers:   1,
		batchSize:    10,
		batchTimeout: "50ms",
	}
}

func newPipeline(
	t *testing.T,
	opts pipelineOpts,
	sink sinks.Sink,
	dlqImpl dlq.DeadLetterQueue,
) (svc *ingestor.Service, cancel context.CancelFunc, wg *sync.WaitGroup) {
	t.Helper()

	buf := buffer.NewChannelBuffer(opts.bufSize)

	var w wal.WAL
	if opts.walDir != "" {
		fw, err := wal.NewFileWAL(opts.walDir)
		if err != nil {
			t.Fatalf("newPipeline: create WAL: %v", err)
		}
		t.Cleanup(func() {
			err := fw.Close()
			if err != nil {
				t.Fatalf("newPipeline: cleanup: %v", err)
			}
		})
		w = fw
	}

	svc = ingestor.NewService(buf, metrics.NewMock(), w)

	ctx, cancelFn := context.WithCancel(context.Background())

	wg = worker.Start(
		ctx,
		opts.numWorkers,
		svc.Buf().Chan(),
		opts.batchSize,
		opts.batchTimeout,
		[]sinks.Sink{sink},
		metrics.NewMock(),
		dlqImpl,
		svc.WAL(),
		svc.SeqTracker(),
	)

	cancel = func() {
		cancelFn()
		svc.Close()
	}

	return svc, cancel, wg
}

func TestIntegration_HTTP_Ingest_ReachesWorker(t *testing.T) {
	mockSink := sinks.NewMockSink()
	svc, cancel, wg := newPipeline(t, defaultOpts(), mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	hs, err := server.NewHttpServer(0, svc, true)
	if err != nil {
		t.Fatalf("NewHttpServer: %v", err)
	}

	ts := httptest.NewServer(hs.Handler())
	defer ts.Close()

	body := `{"event_id":"http-e2e-1","source":"integration","payload":"{}","timestamp":1}`
	resp, err := http.Post(ts.URL+"/ingest", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("POST /ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if result.Status != "Accepted" {
		t.Errorf("expected status Accepted, got %q", result.Status)
	}

	if !waitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == 1
	}) {
		t.Fatalf("event never reached sink (got %d events)", mockSink.TotalEvents())
	}

	batches := mockSink.GetBatches()
	if len(batches) == 0 || batches[0][0].EventId != "http-e2e-1" {
		t.Errorf("expected event_id=http-e2e-1, got %+v", batches)
	}
}

func TestIntegration_gRPC_Ingest_ReachesWorker(t *testing.T) {
	mockSink := sinks.NewMockSink()
	svc, cancel, wg := newPipeline(t, defaultOpts(), mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	port := getFreePort(t)
	gs, err := server.NewGrpcServer(port, svc, metrics.NewMock(), true)
	if err != nil {
		t.Fatalf("NewGrpcServer: %v", err)
	}
	if err := gs.Start(); err != nil {
		t.Fatalf("GrpcServer.Start: %v", err)
	}
	defer gs.Stop()

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	client := pb.NewIngestorServiceClient(conn)

	ctx, cancelRPC := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRPC()

	resp, err := client.Ingest(ctx, &pb.IngestRequest{
		EventId:   "grpc-e2e-1",
		Source:    "integration",
		Payload:   `{"key":"value"}`,
		Timestamp: 2,
	})
	if err != nil {
		t.Fatalf("Ingest RPC: %v", err)
	}
	if resp.Status != "OK" {
		t.Errorf("expected status OK, got %q (error: %s)", resp.Status, resp.Error)
	}

	if !waitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == 1
	}) {
		t.Fatalf("event never reached sink (got %d events)", mockSink.TotalEvents())
	}

	batches := mockSink.GetBatches()
	if len(batches) == 0 || batches[0][0].EventId != "grpc-e2e-1" {
		t.Errorf("expected event_id=grpc-e2e-1, got %+v", batches)
	}
}

func TestIntegration_WAL_RecoverAfterCrash(t *testing.T) {
	walDir := t.TempDir()

	fw1, err := wal.NewFileWAL(walDir)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}

	buf := buffer.NewChannelBuffer(50)
	svc := ingestor.NewService(buf, metrics.NewMock(), fw1)

	eventIDs := []string{"crash-1", "crash-2", "crash-3"}
	for _, id := range eventIDs {
		if err := svc.Push(&pb.IngestRequest{EventId: id, Source: "crash-test"}); err != nil {
			t.Fatalf("Push(%s): %v", id, err)
		}
	}

	if err := fw1.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}

	fw2, err := wal.NewFileWAL(walDir)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer fw2.Close()

	entries, err := fw2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if got, want := len(entries), len(eventIDs); got != want {
		t.Fatalf("Recover returned %d entries, want %d", got, want)
	}

	recovered := make(map[string]bool)
	for _, e := range entries {
		recovered[e.Event.EventId] = true
	}
	for _, id := range eventIDs {
		if !recovered[id] {
			t.Errorf("event %q missing from recovered entries", id)
		}
	}
}

func TestIntegration_WAL_AcksOnSuccess(t *testing.T) {
	walDir := t.TempDir()
	opts := defaultOpts()
	opts.walDir = walDir
	opts.batchTimeout = "30ms"

	mockSink := sinks.NewMockSink()
	svc, cancel, wg := newPipeline(t, opts, mockSink, dlq.NewNoOpDLQ())

	eventIDs := []string{"ack-1", "ack-2", "ack-3"}
	for _, id := range eventIDs {
		if err := svc.Push(&pb.IngestRequest{EventId: id, Source: "ack-test"}); err != nil {
			t.Fatalf("Push(%s): %v", id, err)
		}
	}

	if !waitFor(t, 3*time.Second, 20*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == len(eventIDs)
	}) {
		t.Fatalf("not all events reached sink: got %d, want %d", mockSink.TotalEvents(), len(eventIDs))
	}

	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()

	fw, err := wal.NewFileWAL(walDir)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	defer fw.Close()

	entries, err := fw.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Recover returned %d unacked entries, want 0; events: %v",
			len(entries), func() []string {
				ids := make([]string, len(entries))
				for i, e := range entries {
					ids[i] = e.Event.EventId
				}
				return ids
			}())
	}
}

func TestIntegration_SinkFailure_SendsToDLQ(t *testing.T) {
	failSink := sinks.NewMockSink()
	failSink.SetError(errors.New("sink unavailable"))

	capDLQ := &capturingDLQ{}

	opts := defaultOpts()
	opts.batchSize = 3
	opts.batchTimeout = "50ms"
	opts.numWorkers = 1

	svc, cancel, wg := newPipeline(t, opts, failSink, capDLQ)
	defer func() {
		cancel()
		wg.Wait()
	}()

	eventIDs := []string{"dlq-1", "dlq-2", "dlq-3"}
	for _, id := range eventIDs {
		if err := svc.Push(&pb.IngestRequest{EventId: id, Source: "dlq-test"}); err != nil {
			t.Fatalf("Push(%s): %v", id, err)
		}
	}

	if !waitFor(t, 5*time.Second, 50*time.Millisecond, func() bool {
		return capDLQ.Len() == len(eventIDs)
	}) {
		t.Fatalf("DLQ has %d entries, want %d", capDLQ.Len(), len(eventIDs))
	}

	dlqIDs := capDLQ.EventIDs()
	dlqSet := make(map[string]bool, len(dlqIDs))
	for _, id := range dlqIDs {
		dlqSet[id] = true
	}
	for _, id := range eventIDs {
		if !dlqSet[id] {
			t.Errorf("event %q not found in DLQ", id)
		}
	}
}

func TestIntegration_GracefulShutdown_DrainAll(t *testing.T) {
	const totalEvents = 50

	mockSink := sinks.NewMockSink()
	buf := buffer.NewChannelBuffer(200)
	svc := ingestor.NewService(buf, metrics.NewMock(), nil)

	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	wg := worker.Start(
		ctx,
		3,
		svc.Buf().Chan(),
		5,
		"100ms",
		[]sinks.Sink{mockSink},
		metrics.NewMock(),
		dlq.NewNoOpDLQ(),
		nil,
		nil,
	)

	for i := 0; i < totalEvents; i++ {
		if err := svc.Push(&pb.IngestRequest{
			EventId: fmt.Sprintf("drain-%d", i),
			Source:  "shutdown-test",
		}); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	svc.Close()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not drain within timeout after buffer close")
	}

	if got := mockSink.TotalEvents(); got != totalEvents {
		t.Errorf("expected all %d events at sink, got %d", totalEvents, got)
	}
}

func TestIntegration_BatchSize_TriggersFlush(t *testing.T) {
	mockSink := sinks.NewMockSink()

	opts := defaultOpts()
	opts.batchSize = 4
	opts.batchTimeout = "10s"
	opts.numWorkers = 1

	svc, cancel, wg := newPipeline(t, opts, mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	for i := 0; i < 4; i++ {
		if err := svc.Push(&pb.IngestRequest{
			EventId: fmt.Sprintf("bs-%d", i),
			Source:  "batch-size-test",
		}); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	if !waitFor(t, 2*time.Second, 20*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == 4
	}) {
		t.Fatalf("batch size flush did not fire: got %d events at sink", mockSink.TotalEvents())
	}

	if got := mockSink.BatchCount(); got != 1 {
		t.Errorf("expected 1 batch, got %d", got)
	}
}

func TestIntegration_BatchTimeout_TriggersFlush(t *testing.T) {
	mockSink := sinks.NewMockSink()

	opts := defaultOpts()
	opts.batchSize = 100
	opts.batchTimeout = "60ms"
	opts.numWorkers = 1

	svc, cancel, wg := newPipeline(t, opts, mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	const pushed = 3
	for i := 0; i < pushed; i++ {
		if err := svc.Push(&pb.IngestRequest{
			EventId: fmt.Sprintf("to-%d", i),
			Source:  "timeout-test",
		}); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	if !waitFor(t, 2*time.Second, 20*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == pushed
	}) {
		t.Fatalf("timeout flush did not fire: got %d events at sink, want %d",
			mockSink.TotalEvents(), pushed)
	}

	if mockSink.TotalEvents() != pushed {
		t.Errorf("expected %d events, got %d", pushed, mockSink.TotalEvents())
	}
}

func TestIntegration_gRPC_IngestDisabled(t *testing.T) {
	mockSink := sinks.NewMockSink()
	svc, cancel, wg := newPipeline(t, defaultOpts(), mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	port := getFreePort(t)
	gs, err := server.NewGrpcServer(port, svc, metrics.NewMock(), false)
	if err != nil {
		t.Fatalf("NewGrpcServer: %v", err)
	}
	if err := gs.Start(); err != nil {
		t.Fatalf("GrpcServer.Start: %v", err)
	}
	defer gs.Stop()

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancelRPC := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRPC()

	resp, err := pb.NewIngestorServiceClient(conn).Ingest(ctx, &pb.IngestRequest{
		EventId: "disabled-1",
		Source:  "test",
	})
	if err != nil {
		t.Fatalf("Ingest RPC: %v", err)
	}
	if resp.Status != "DISABLED" {
		t.Errorf("expected DISABLED, got %q", resp.Status)
	}

	time.Sleep(100 * time.Millisecond)
	if got := mockSink.TotalEvents(); got != 0 {
		t.Errorf("expected 0 events at sink when disabled, got %d", got)
	}
}

func TestIntegration_MultipleWorkers_AllEventsDelivered(t *testing.T) {
	const totalEvents = 200

	mockSink := sinks.NewMockSink()

	opts := defaultOpts()
	opts.bufSize = 500
	opts.numWorkers = 4
	opts.batchSize = 10
	opts.batchTimeout = "50ms"

	svc, cancel, wg := newPipeline(t, opts, mockSink, dlq.NewNoOpDLQ())
	defer func() {
		cancel()
		wg.Wait()
	}()

	for i := 0; i < totalEvents; i++ {
		if err := svc.Push(&pb.IngestRequest{
			EventId: fmt.Sprintf("multi-%d", i),
			Source:  "multi-worker-test",
		}); err != nil {
			t.Fatalf("Push(%d): %v", i, err)
		}
	}

	if !waitFor(t, 5*time.Second, 30*time.Millisecond, func() bool {
		return mockSink.TotalEvents() == totalEvents
	}) {
		t.Fatalf("expected %d events at sink, got %d", totalEvents, mockSink.TotalEvents())
	}
}
