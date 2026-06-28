package server

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/i18n"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/response"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

// Ingest godoc
//
//	@Summary		Ingest an event
//	@Description	Accepts a single event for asynchronous processing. The event is buffered
//	@Description	in memory (and optionally a write-ahead log) and forwarded to the
//	@Description	configured sinks by background workers. The response is returned before
//	@Description	the event has been written to any sink — this is a fire-and-forget API.
//	@Tags			ingest
//	@Accept			json
//	@Produce		json
//	@Param			Accept-Language	header	string				false	"Response language (en, fa). Defaults to en."
//	@Param			input			body	httpIngestRequest	true	"Event to ingest"
//	@Success		202	{object}	response.Envelope		"Event accepted into the buffer"
//	@Failure		400	{object}	response.ErrorEnvelope	"Invalid JSON or missing event_id"
//	@Failure		405	{object}	response.ErrorEnvelope	"Method not allowed (only POST is supported)"
//	@Failure		500	{object}	response.ErrorEnvelope	"Ingestor not configured"
//	@Failure		503	{object}	response.ErrorEnvelope	"Buffer is full, or ingest is currently disabled"
//	@Router			/ingest [post]
func (s *HttpServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		response.Error(w, r, http.StatusMethodNotAllowed, i18n.MsgMethodNotAllowed)
		return
	}
	if !s.ingestEnabled.Load() {
		response.Error(w, r, http.StatusServiceUnavailable, i18n.MsgDisabled)
		return
	}
	if s.ingestor == nil {
		response.Error(w, r, http.StatusInternalServerError, i18n.MsgNotConfigured)
		return
	}

	var body httpIngestRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		response.Error(w, r, http.StatusBadRequest, i18n.MsgInvalidJSON)
		return
	}
	if body.EventID == "" {
		lang := i18n.DetectLangFromRequest(r)
		response.Error(w, r, http.StatusBadRequest, i18n.MsgMissingEventID, response.FieldError{
			Field:   "event_id",
			Message: i18n.Translate(lang, i18n.MsgMissingEventID),
		})
		return
	}

	req := &pb.IngestRequest{
		EventId:   body.EventID,
		Source:    body.Source,
		Payload:   body.Payload,
		Timestamp: body.Timestamp,
	}
	if err := s.ingestor.Push(req); err != nil {
		response.Error(w, r, http.StatusServiceUnavailable, i18n.MsgDropped)
		return
	}

	response.JSON(w, r, http.StatusAccepted, httpIngestResponse{EventID: body.EventID}, i18n.MsgAccepted)
}

// Health godoc
//
//	@Summary		Health check
//	@Description	Returns 200 as long as the HTTP server is reachable.
//	@Tags			system
//	@Produce		json
//	@Param			Accept-Language	header	string	false	"Response language (en, fa)."
//	@Success		200	{object}	response.Envelope	"Service is healthy"
//	@Router			/health [get]
func (s *HttpServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	response.JSON(w, r, http.StatusOK, healthData{Status: "ok"}, i18n.MsgHealthOK)
}

// Ready godoc
//
//	@Summary		Readiness probe
//	@Description	Returns 200 once workers are running and the service is ready to accept ingest traffic.
//	@Description	Returns 503 during startup and graceful shutdown.
//	@Tags			system
//	@Produce		json
//	@Param			Accept-Language	header	string	false	"Response language (en, fa)."
//	@Success		200	{object}	response.Envelope	"Service is ready"
//	@Failure		503	{object}	response.Envelope	"Service is not ready"
//	@Router			/ready [get]
func (s *HttpServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready.Load() {
		response.JSON(w, r, http.StatusOK, readyData{Ready: true}, i18n.MsgReady)
		return
	}
	response.JSON(w, r, http.StatusServiceUnavailable, readyData{Ready: false}, i18n.MsgNotReady)
}

// DLQReplay godoc
//
//	@Summary		Replay dead-letter queue
//	@Description	Atomically drains all pending DLQ entries and re-pushes every event
//	@Description	through the ingest buffer for reprocessing.
//	@Description	Only supported when the configured DLQ backend is FileDLQ.
//	@Tags			admin
//	@Produce		json
//	@Param			Accept-Language	header	string	false	"Response language (en, fa)."
//	@Success		200	{object}	response.Envelope		"Replay complete"
//	@Failure		405	{object}	response.ErrorEnvelope	"Method not allowed"
//	@Failure		500	{object}	response.ErrorEnvelope	"Failed to drain DLQ entries"
//	@Failure		501	{object}	response.ErrorEnvelope	"DLQ backend does not support replay"
//	@Router			/admin/dlq/replay [post]
func (s *HttpServer) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		response.Error(w, r, http.StatusMethodNotAllowed, i18n.MsgMethodNotAllowed)
		return
	}

	replayable, ok := s.dlq.(dlq.Replayable)
	if !ok {
		response.Error(w, r, http.StatusNotImplemented, i18n.MsgDLQNotReplayable)
		return
	}

	entries, err := replayable.DrainEntries()
	if err != nil {
		slog.Error("DLQ replay: drain failed", "err", err)
		response.Error(w, r, http.StatusInternalServerError, i18n.MsgDLQReplayFailed)
		return
	}

	if len(entries) == 0 {
		response.JSON(w, r, http.StatusOK, dlqReplayData{}, i18n.MsgDLQReplayEmpty)
		return
	}

	replayed, failed := 0, 0
	for _, entry := range entries {
		if pushErr := s.ingestor.Push(entry.Event); pushErr != nil {
			slog.Warn("DLQ replay: push failed", "event_id", entry.Event.EventId, "err", pushErr)
			failed++
		} else {
			replayed++
		}
	}

	slog.Info("DLQ replay finished", "total", len(entries), "replayed", replayed, "failed", failed)

	code := i18n.MsgDLQReplayOK
	if failed > 0 {
		code = i18n.MsgDLQReplayPartial
	}
	response.JSON(w, r, http.StatusOK, dlqReplayData{
		Replayed: replayed,
		Failed:   failed,
		Total:    len(entries),
	}, code)
}

// DLQStats godoc
//
//	@Summary		DLQ statistics
//	@Description	Returns a snapshot of all pending DLQ files: names, sizes, entry counts.
//	@Description	Only supported when the configured DLQ backend is FileDLQ.
//	@Tags			admin
//	@Produce		json
//	@Param			Accept-Language	header	string	false	"Response language (en, fa)."
//	@Success		200	{object}	response.Envelope		"Stats retrieved"
//	@Failure		405	{object}	response.ErrorEnvelope	"Method not allowed"
//	@Failure		500	{object}	response.ErrorEnvelope	"Failed to read DLQ stats"
//	@Failure		501	{object}	response.ErrorEnvelope	"DLQ backend does not support stats"
//	@Router			/admin/dlq/stats [get]
func (s *HttpServer) handleDLQStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		response.Error(w, r, http.StatusMethodNotAllowed, i18n.MsgMethodNotAllowed)
		return
	}

	replayable, ok := s.dlq.(dlq.Replayable)
	if !ok {
		response.Error(w, r, http.StatusNotImplemented, i18n.MsgDLQStatsUnsupported)
		return
	}

	stats, err := replayable.Stats()
	if err != nil {
		slog.Error("DLQ stats failed", "err", err)
		response.Error(w, r, http.StatusInternalServerError, i18n.MsgDLQStatsFailed)
		return
	}

	response.JSON(w, r, http.StatusOK, stats, i18n.MsgDLQStatsOK)
}