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
	var resp struct {
		Data struct {
			EventID string `json:"event_id"`
		} `json:"data"`
		Meta struct {
			MessageCode string `json:"messageCode"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Meta.MessageCode != "ACCEPTED" {
		t.Errorf("expected messageCode ACCEPTED, got %q", resp.Meta.MessageCode)
	}
	if resp.Data.EventID != "e1" {
		t.Errorf("expected data.event_id=e1, got %q", resp.Data.EventID)
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
	var resp struct {
		Meta struct {
			MessageCode string `json:"messageCode"`
		} `json:"meta"`
	}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Meta.MessageCode != "DROPPED" {
		t.Errorf("expected messageCode DROPPED, got %q", resp.Meta.MessageCode)
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
	t.Cleanup(func() {
		err := d.Close()
		if err != nil {
			return
		}
	})
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
	var env struct {
		Data dlqReplayData `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Data.Total != 0 {
		t.Errorf("expected total=0, got %d", env.Data.Total)
	}
}

func TestHandleDLQReplay_RequeuesEntries(t *testing.T) {
	fileDLQ := newTestFileDLQ(t)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		func() {
			err := fileDLQ.Push(ctx, &pb.IngestRequest{
				EventId: fmt.Sprintf("replay-%d", i),
				Source:  "test",
			}, "TestSink", errors.New("sink down"))
			if err != nil {
				return
			}
		}()
	}

	hs := newTestServer(t, true, 10)
	hs.SetDLQ(fileDLQ)

	w := httptest.NewRecorder()
	hs.handleDLQReplay(w, httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var env struct {
		Data dlqReplayData `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("json.Decode: %v", err)
	}

	if env.Data.Replayed != 3 {
		t.Errorf("expected replayed=3, got %d", env.Data.Replayed)
	}
	if env.Data.Failed != 0 {
		t.Errorf("expected failed=0, got %d", env.Data.Failed)
	}
	if env.Data.Total != 3 {
		t.Errorf("expected total=3, got %d", env.Data.Total)
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
	if err := fileDLQ.Push(ctx, &pb.IngestRequest{EventId: "s-1", Source: "test"}, "Sink", errors.New("err")); err != nil {
		t.Fatalf("fileDLQ.Push s-1: %v", err)
	}
	if err := fileDLQ.Push(ctx, &pb.IngestRequest{EventId: "s-2", Source: "test"}, "Sink", errors.New("err")); err != nil {
		t.Fatalf("fileDLQ.Push s-2: %v", err)
	}

	hs := newTestServer(t, true, 10)
	hs.SetDLQ(fileDLQ)

	w := httptest.NewRecorder()
	hs.handleDLQStats(w, httptest.NewRequest(http.MethodGet, "/admin/dlq/stats", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var env struct {
		Data dlq.DLQStats `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatalf("json.Decode: %v", err)
	}
	if env.Data.TotalEntries != 2 {
		t.Errorf("expected 2 entries, got %d", env.Data.TotalEntries)
	}
}

func TestHandleDLQReplay_AdminToken(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetDLQ(newTestFileDLQ(t))
	hs.SetAdminToken("s3cret")

	w := httptest.NewRecorder()
	hs.handleDLQReplay(w, httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	rWrong := httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil)
	rWrong.Header.Set("Authorization", "Bearer nope")
	hs.handleDLQReplay(w, rWrong)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", w.Code)
	}

	w = httptest.NewRecorder()
	rOK := httptest.NewRequest(http.MethodPost, "/admin/dlq/replay", nil)
	rOK.Header.Set("Authorization", "Bearer s3cret")
	hs.handleDLQReplay(w, rOK)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 with correct token, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestHandleIngest_TenancyRequired_MissingTenant(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetTenancyRequired(true)

	w := doIngest(t, hs, http.MethodPost, `{"event_id":"e1","source":"x"}`)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if resp.Error.Code != "MISSING_TENANT_ID" {
		t.Errorf("expected error code MISSING_TENANT_ID, got %q", resp.Error.Code)
	}
}

func TestHandleIngest_TenancyRequired_ThreadsTenantID(t *testing.T) {
	hs := newTestServer(t, true, 10)
	hs.SetTenancyRequired(true)

	w := doIngest(t, hs, http.MethodPost, `{"event_id":"e1","tenant_id":"acme"}`)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d body=%s", w.Code, w.Body.String())
	}
	req := <-hs.ingestor.Buf().Chan()
	if req.TenantId != "acme" {
		t.Errorf("expected TenantId=acme in buffered event, got %q", req.TenantId)
	}
}

func TestHandleIngest_TenancyDisabled_TenantOptional(t *testing.T) {
	hs := newTestServer(t, true, 10) // requireTenant defaults to false

	w := doIngest(t, hs, http.MethodPost, `{"event_id":"e1"}`)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202 when tenancy disabled, got %d body=%s", w.Code, w.Body.String())
	}
}
