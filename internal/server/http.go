package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpServer struct {
	server        *http.Server
	addr          string
	ready         atomic.Bool
	ingestEnabled atomic.Bool
	ingestor      *ingestor.Service
	dlq           dlq.DeadLetterQueue
}

func (s *HttpServer) Handler() http.Handler {
	return s.server.Handler
}

func NewHttpServer(port int, svc *ingestor.Service, ingestEnabled bool) (*HttpServer, error) {
	addr := fmt.Sprintf(":%d", port)

	s := &HttpServer{
		addr:     addr,
		ingestor: svc,
	}
	s.ingestEnabled.Store(ingestEnabled)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/ingest", s.handleIngest)

	mux.HandleFunc("/admin/dlq/replay", s.handleDLQReplay)
	mux.HandleFunc("/admin/dlq/stats", s.handleDLQStats)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s, nil
}

type httpIngestRequest struct {
	EventID   string `json:"event_id"`
	Source    string `json:"source"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

type httpIngestResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

func (s *HttpServer) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeIngestJSON(w, http.StatusMethodNotAllowed, httpIngestResponse{Status: "ERROR", Error: "method not allowed"})
		return
	}

	if !s.ingestEnabled.Load() {
		writeIngestJSON(w, http.StatusServiceUnavailable, httpIngestResponse{Status: "DISABLED", Error: "ingest disabled"})
		return
	}

	if s.ingestor == nil {
		writeIngestJSON(w, http.StatusInternalServerError, httpIngestResponse{Status: "ERROR", Error: "ingestor not configured"})
		return
	}

	var body httpIngestRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeIngestJSON(w, http.StatusBadRequest, httpIngestResponse{Status: "ERROR", Error: fmt.Sprintf("invalid json: %v", err)})
		return
	}

	if body.EventID == "" {
		writeIngestJSON(w, http.StatusBadRequest, httpIngestResponse{Status: "ERROR", Error: "missing event_id"})
		return
	}

	req := &pb.IngestRequest{
		EventId:   body.EventID,
		Source:    body.Source,
		Payload:   body.Payload,
		Timestamp: body.Timestamp,
	}

	if err := s.ingestor.Push(req); err != nil {
		writeIngestJSON(w, http.StatusServiceUnavailable, httpIngestResponse{Status: "DROPPED", Error: "buffer full"})
		return
	}

	writeIngestJSON(w, http.StatusAccepted, httpIngestResponse{Status: "Accepted"})
}

func writeIngestJSON(w http.ResponseWriter, status int, body httpIngestResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("Failed to encode ingest response", "err", err)
	}
}

func (s *HttpServer) SetIngestEnabled(enabled bool) {
	s.ingestEnabled.Store(enabled)
}

func (s *HttpServer) Start() error {
	slog.Info("HTTP server listening", "addr", s.addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server error", "err", err)
		}
	}()
	return nil
}

func (s *HttpServer) Stop(ctx context.Context) error {
	slog.Info("Shutting down HTTP server")
	return s.server.Shutdown(ctx)
}

func (s *HttpServer) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *HttpServer) Addr() string {
	return s.addr
}

func (s *HttpServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ok")); err != nil {
		slog.Error("Failed to write /health response", "err", err)
	}
}

func (s *HttpServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ready")); err != nil {
			slog.Error("Failed to write /ready response", "err", err)
		}
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	if _, err := w.Write([]byte("not ready")); err != nil {
		slog.Error("Failed to write /ready response", "err", err)
	}
}

func (s *HttpServer) SetDLQ(d dlq.DeadLetterQueue) {
	s.dlq = d
}

type dlqReplayResponse struct {
	Replayed int    `json:"replayed"`
	Failed   int    `json:"failed"`
	Total    int    `json:"total"`
	Message  string `json:"message"`
}

func (s *HttpServer) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	replayable, ok := s.dlq.(dlq.Replayable)
	if !ok {
		writeAdminJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "configured DLQ backend does not support replay",
		})
		return
	}

	entries, err := replayable.DrainEntries()
	if err != nil {
		slog.Error("DLQ replay: drain failed", "err", err)
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if len(entries) == 0 {
		writeAdminJSON(w, http.StatusOK, dlqReplayResponse{
			Total:   0,
			Message: "DLQ is empty, nothing to replay",
		})
		return
	}

	replayed, failed := 0, 0
	for _, entry := range entries {
		if err := s.ingestor.Push(entry.Event); err != nil {
			slog.Warn("DLQ replay: push failed",
				"event_id", entry.Event.EventId,
				"err", err,
			)
			failed++
		} else {
			replayed++
		}
	}

	msg := "Replay complete"
	if failed > 0 {
		msg = fmt.Sprintf("Replay partial: %d/%d events failed to re-queue (buffer full)", failed, len(entries))
	}

	slog.Info("DLQ replay finished", "total", len(entries), "replayed", replayed, "failed", failed)
	writeAdminJSON(w, http.StatusOK, dlqReplayResponse{
		Replayed: replayed,
		Failed:   failed,
		Total:    len(entries),
		Message:  msg,
	})
}

func (s *HttpServer) handleDLQStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeAdminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	replayable, ok := s.dlq.(dlq.Replayable)
	if !ok {
		writeAdminJSON(w, http.StatusNotImplemented, map[string]string{
			"error": "configured DLQ backend does not support stats",
		})
		return
	}

	stats, err := replayable.Stats()
	if err != nil {
		slog.Error("DLQ stats failed", "err", err)
		writeAdminJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeAdminJSON(w, http.StatusOK, stats)
}

func writeAdminJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Error("Failed to encode admin response", "err", err)
	}
}
