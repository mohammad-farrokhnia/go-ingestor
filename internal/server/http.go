package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpServer struct {
	server        *http.Server
	addr          string
	ready         atomic.Bool
	ingestEnabled atomic.Bool
	requireTenant atomic.Bool
	ingestor      *ingestor.Service
	dlq           dlq.DeadLetterQueue
}

type healthData struct {
	Status string `json:"status" example:"ok"`
}

type readyData struct {
	Ready bool `json:"ready" example:"true"`
}

type httpIngestRequest struct {
	EventID   string `json:"event_id" example:"evt-12345"`
	Source    string `json:"source"   example:"web"`
	Payload   string `json:"payload"  example:"{\"key\":\"value\"}"`
	Timestamp int64  `json:"timestamp" example:"1718000000"`
	TenantID  string `json:"tenant_id" example:"acme"`
}

type httpIngestResponse struct {
	EventID string `json:"event_id" example:"evt-12345"`
}

type dlqReplayData struct {
	Replayed int `json:"replayed" example:"12"`
	Failed   int `json:"failed"   example:"0"`
	Total    int `json:"total"    example:"12"`
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

func (s *HttpServer) SetIngestEnabled(enabled bool) {
	s.ingestEnabled.Store(enabled)
}

// SetTenancyRequired toggles whether a non-empty tenant_id is required on
// every ingested event. When false (the default) tenant_id is optional.
func (s *HttpServer) SetTenancyRequired(required bool) {
	s.requireTenant.Store(required)
}

func (s *HttpServer) SetDLQ(d dlq.DeadLetterQueue) {
	s.dlq = d
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

func (s *HttpServer) SetReady(ready bool) { s.ready.Store(ready) }

func (s *HttpServer) Handler() http.Handler { return s.server.Handler }

func (s *HttpServer) Addr() string { return s.addr }
