package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

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
