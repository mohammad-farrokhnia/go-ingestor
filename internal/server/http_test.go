package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
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
