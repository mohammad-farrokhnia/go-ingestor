package response

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/i18n"
)

var (
	appName string
	version string
)

func Init(name, ver string) {
	appName = name
	version = ver
}

const requestIDHeader = "X-Request-Id"

type Meta struct {
	AppName     string `json:"appName"     example:"go-ingestor"`
	Version     string `json:"version"     example:"v1.0.0"`
	RequestID   string `json:"requestId"   example:"550e8400-e29b-41d4-a716-446655440000"`
	Timestamp   string `json:"timestamp"   example:"2026-06-15T10:00:00Z"`
	MessageCode string `json:"messageCode" example:"ACCEPTED"`
	Message     string `json:"message"     example:"Event accepted."`
	Lang        string `json:"lang"        example:"en"`
}

type FieldError struct {
	Field   string `json:"field"   example:"event_id"`
	Message string `json:"message" example:"Field 'event_id' is required."`
}

type ErrorBody struct {
	Code   string       `json:"code"             example:"MISSING_EVENT_ID"`
	Fields []FieldError `json:"fields,omitempty"`
}

type Envelope struct {
	Data any  `json:"data"`
	Meta Meta `json:"meta"`
}

type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
	Meta  Meta      `json:"meta"`
}

func newMeta(w http.ResponseWriter, r *http.Request, code i18n.MessageCode) Meta {
	lang := i18n.DetectLangFromRequest(r)

	reqID := r.Header.Get(requestIDHeader)
	if reqID == "" {
		reqID = uuid.NewString()
	}
	w.Header().Set(requestIDHeader, reqID)

	return Meta{
		AppName:     appName,
		Version:     version,
		RequestID:   reqID,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		MessageCode: string(code),
		Message:     i18n.Translate(lang, code),
		Lang:        string(lang),
	}
}

func JSON(w http.ResponseWriter, r *http.Request, status int, data any, code i18n.MessageCode) {
	writeJSON(w, status, Envelope{Data: data, Meta: newMeta(w, r, code)})
}

func Error(w http.ResponseWriter, r *http.Request, status int, code i18n.MessageCode, fields ...FieldError) {
	writeJSON(w, status, ErrorEnvelope{
		Error: ErrorBody{Code: string(code), Fields: fields},
		Meta:  newMeta(w, r, code),
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("response: failed to encode JSON body", "err", err)
	}
}
