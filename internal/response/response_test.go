package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/i18n"
)

func TestJSON_WritesSuccessEnvelope(t *testing.T) {
	Init("go-ingestor-test", "v0.0.0-test")

	req := httptest.NewRequest(http.MethodPost, "/ingest", nil)
	w := httptest.NewRecorder()

	type payload struct {
		EventID string `json:"event_id"`
	}
	JSON(w, req, http.StatusAccepted, payload{EventID: "evt-1"}, i18n.MsgAccepted)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	var got struct {
		Data payload `json:"data"`
		Meta Meta    `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Data.EventID != "evt-1" {
		t.Errorf("data.event_id = %q, want evt-1", got.Data.EventID)
	}
	if got.Meta.AppName != "go-ingestor-test" {
		t.Errorf("meta.appName = %q, want go-ingestor-test", got.Meta.AppName)
	}
	if got.Meta.MessageCode != string(i18n.MsgAccepted) {
		t.Errorf("meta.messageCode = %q, want %q", got.Meta.MessageCode, i18n.MsgAccepted)
	}
	if got.Meta.Lang != "en" {
		t.Errorf("meta.lang = %q, want en", got.Meta.Lang)
	}
	if got.Meta.RequestID == "" {
		t.Error("meta.requestId should not be empty")
	}
	if got.Meta.Timestamp == "" {
		t.Error("meta.timestamp should not be empty")
	}
}

func TestError_WritesErrorEnvelopeWithFields(t *testing.T) {
	Init("go-ingestor-test", "v0.0.0-test")

	req := httptest.NewRequest(http.MethodPost, "/ingest", nil)
	w := httptest.NewRecorder()

	Error(w, req, http.StatusBadRequest, i18n.MsgMissingEventID, FieldError{
		Field:   "event_id",
		Message: "Field 'event_id' is required.",
	})

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	var got struct {
		Error ErrorBody `json:"error"`
		Meta  Meta      `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Error.Code != string(i18n.MsgMissingEventID) {
		t.Errorf("error.code = %q, want %q", got.Error.Code, i18n.MsgMissingEventID)
	}
	if len(got.Error.Fields) != 1 || got.Error.Fields[0].Field != "event_id" {
		t.Errorf("error.fields = %+v, want one entry for event_id", got.Error.Fields)
	}
}

func TestNewMeta_PropagatesIncomingRequestID(t *testing.T) {
	Init("go-ingestor-test", "v0.0.0-test")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Request-Id", "caller-supplied-id")
	w := httptest.NewRecorder()

	JSON(w, req, http.StatusOK, nil, i18n.MsgHealthOK)

	if got := w.Header().Get("X-Request-Id"); got != "caller-supplied-id" {
		t.Errorf("response X-Request-Id = %q, want caller-supplied-id", got)
	}
	var body struct {
		Meta Meta `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Meta.RequestID != "caller-supplied-id" {
		t.Errorf("meta.requestId = %q, want caller-supplied-id", body.Meta.RequestID)
	}
}

func TestNewMeta_GeneratesRequestIDWhenAbsent(t *testing.T) {
	Init("go-ingestor-test", "v0.0.0-test")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	JSON(w, req, http.StatusOK, nil, i18n.MsgHealthOK)

	headerID := w.Header().Get("X-Request-Id")
	if headerID == "" {
		t.Error("expected a generated X-Request-Id header")
	}
	var body struct {
		Meta Meta `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Meta.RequestID != headerID {
		t.Errorf("meta.requestId = %q, want header %q", body.Meta.RequestID, headerID)
	}
}

func TestJSON_RespectsAcceptLanguage(t *testing.T) {
	Init("go-ingestor-test", "v0.0.0-test")

	req := httptest.NewRequest(http.MethodPost, "/ingest", nil)
	req.Header.Set("Accept-Language", "fa")
	w := httptest.NewRecorder()

	JSON(w, req, http.StatusAccepted, nil, i18n.MsgAccepted)

	var body struct {
		Meta Meta `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Meta.Lang != "fa" {
		t.Errorf("meta.lang = %q, want fa", body.Meta.Lang)
	}
	if body.Meta.Message != i18n.Translate(i18n.LangFA, i18n.MsgAccepted) {
		t.Errorf("meta.message = %q, want Persian translation", body.Meta.Message)
	}
}
