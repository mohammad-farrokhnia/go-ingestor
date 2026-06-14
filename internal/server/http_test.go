package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func newTestServer(t *testing.T, ingestEnabled bool, bufferSize int) *HttpServer {
	t.Helper()
	buf := buffer.NewChannelBuffer(bufferSize)
	svc := ingestor.NewService(buf, metrics.NewMock(), nil)
	hs, err := NewHttpServer(0, svc, ingestEnabled)
	if err != nil {
		t.Fatalf("NewHttpServer: %v", err)
	}
	return hs
}

func doIngest(t *testing.T, hs *HttpServer, method string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/ingest", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	hs.handleIngest(w, req)
	return w
}

func TestHandleIngest_Success(t *testing.T) {
	hs := newTestServer(t, true, 10)

	body := `{"event_id":"e1","source":"unit","payload":"hello","timestamp":1}`
	w := doIngest(t, hs, http.MethodPost, body)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp httpIngestResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Status != "Accepted" {
		t.Errorf("expected status Accepted, got %q", resp.Status)
	}
	if got := hs.ingestor.Buf().Len(); got != 1 {
		t.Errorf("expected 1 event in buffer, got %d", got)
	}
}

func TestHandleIngest_MethodNotAllowed(t *testing.T) {
	hs := newTestServer(t, true, 10)
	w := doIngest(t, hs, http.MethodGet, "")
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandleIngest_Disabled(t *testing.T) {
	hs := newTestServer(t, false, 10)
	w := doIngest(t, hs, http.MethodPost, `{"event_id":"e1"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when disabled, got %d", w.Code)
	}
}

func TestHandleIngest_MissingEventID(t *testing.T) {
	hs := newTestServer(t, true, 10)
	w := doIngest(t, hs, http.MethodPost, `{"source":"x"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleIngest_InvalidJSON(t *testing.T) {
	hs := newTestServer(t, true, 10)
	w := doIngest(t, hs, http.MethodPost, `{not-json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleIngest_UnknownField(t *testing.T) {
	hs := newTestServer(t, true, 10)
	w := doIngest(t, hs, http.MethodPost, `{"event_id":"e1","extra":"bad"}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown field, got %d", w.Code)
	}
}

func TestHandleIngest_BufferFull(t *testing.T) {
	hs := newTestServer(t, true, 1)
	w1 := doIngest(t, hs, http.MethodPost, `{"event_id":"e1"}`)
	if w1.Code != http.StatusAccepted {
		t.Fatalf("first push expected 202, got %d", w1.Code)
	}
	w2 := doIngest(t, hs, http.MethodPost, `{"event_id":"e2"}`)
	if w2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when buffer full, got %d", w2.Code)
	}
	var resp httpIngestResponse
	_ = json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp.Status != "DROPPED" {
		t.Errorf("expected DROPPED, got %q", resp.Status)
	}
}

func TestHandleIngest_BodySizeLimit(t *testing.T) {
	hs := newTestServer(t, true, 10)
	big := bytes.Repeat([]byte("a"), 1<<20+10)
	body := `{"event_id":"e1","payload":"` + string(big) + `"}`
	w := doIngest(t, hs, http.MethodPost, body)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for oversized body, got %d", w.Code)
	}
}

func TestHandleHealthAndReady(t *testing.T) {
	hs := newTestServer(t, true, 10)

	wHealth := httptest.NewRecorder()
	hs.handleHealth(wHealth, httptest.NewRequest(http.MethodGet, "/health", nil))
	if wHealth.Code != http.StatusOK {
		t.Errorf("expected 200 from /health, got %d", wHealth.Code)
	}

	wNotReady := httptest.NewRecorder()
	hs.handleReady(wNotReady, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if wNotReady.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 from /ready when not ready, got %d", wNotReady.Code)
	}

	hs.SetReady(true)
	wReady := httptest.NewRecorder()
	hs.handleReady(wReady, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if wReady.Code != http.StatusOK {
		t.Errorf("expected 200 from /ready when ready, got %d", wReady.Code)
	}
}

func newTestFileDLQ(t *testing.T) dlq.DeadLetterQueue {
	t.Helper()
	d, err := dlq.NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    "file",
		File:    config.FileDLQConfig{Dir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("create test FileDLQ: %v", err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func TestHandleDLQReplay_EmptyDLQ(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetDLQ(newTestFileDLQ(t))

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil)
	hs.handleDLQReplay(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp dlqReplayResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 0 {
		t.Errorf("expected total=0, got %d", resp.Total)
	}
}

func TestHandleDLQReplay_RequeuesEntries(t *testing.T) {
	fileDLQ := newTestFileDLQ(t)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		fileDLQ.Push(ctx, &pb.IngestRequest{
			EventId: fmt.Sprintf("replay-%d", i),
			Source:  "test",
		}, "TestSink", errors.New("sink down"))
	}

	hs := newTestServer(t, true, 10)
	hs.SetDLQ(fileDLQ)

	w := httptest.NewRecorder()
	hs.handleDLQReplay(w, httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp dlqReplayResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if resp.Replayed != 3 {
		t.Errorf("expected replayed=3, got %d", resp.Replayed)
	}
	if resp.Failed != 0 {
		t.Errorf("expected failed=0, got %d", resp.Failed)
	}
	if resp.Total != 3 {
		t.Errorf("expected total=3, got %d", resp.Total)
	}
}

func TestHandleDLQReplay_WrongMethod(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetDLQ(newTestFileDLQ(t))

	w := httptest.NewRecorder()
	hs.handleDLQReplay(w, httptest.NewRequest(http.MethodGet, "/admin/dlq/replay", nil))

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleDLQReplay_NonReplayableDLQ_Returns501(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetDLQ(dlq.NewNoOpDLQ()) // NoOpDLQ does not implement Replayable

	w := httptest.NewRecorder()
	hs.handleDLQReplay(w, httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil))

	if w.Code != http.StatusNotImplemented {
		t.Errorf("expected 501, got %d", w.Code)
	}
}

func TestHandleDLQStats(t *testing.T) {
	fileDLQ := newTestFileDLQ(t)

	ctx := context.Background()
	fileDLQ.Push(ctx, &pb.IngestRequest{EventId: "s-1", Source: "test"}, "Sink", errors.New("err"))
	fileDLQ.Push(ctx, &pb.IngestRequest{EventId: "s-2", Source: "test"}, "Sink", errors.New("err"))

	hs := newTestServer(t, true, 10)
	hs.SetDLQ(fileDLQ)

	w := httptest.NewRecorder()
	hs.handleDLQStats(w, httptest.NewRequest(http.MethodGet, "/admin/dlq/stats", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var stats dlq.DLQStats
	json.NewDecoder(w.Body).Decode(&stats)
	if stats.TotalEntries != 2 {
		t.Errorf("expected 2 entries, got %d", stats.TotalEntries)
	}
}