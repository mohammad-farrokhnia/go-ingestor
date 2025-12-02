package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type HttpServer struct {
	server *http.Server
	addr   string
	ready  atomic.Bool
}

func NewHttpServer(port int) (*HttpServer, error) {
	addr := fmt.Sprintf(":%d", port)

	s := &HttpServer{
		addr: addr,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return s, nil
}

func (s *HttpServer) Start() error {
	log.Printf("HTTP server listening on %s", s.addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server error: %v", err)
		}
	}()
	return nil
}

func (s *HttpServer) Stop(ctx context.Context) error {
	log.Println("Shutting down HTTP server...")
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
	_, err := w.Write([]byte("ok"))
	if err != nil {
		log.Fatalf("Failed to write response for http request(/health): %v", err)
	}
}

func (s *HttpServer) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready.Load() {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("ready"))
		if err != nil {
			log.Fatalf("Failed to write response for http request(/ready): %v", err)
		}
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, err := w.Write([]byte("not ready"))
		if err != nil {
			log.Fatalf("Failed to write response for http request(/ready): %v", err)
		}
	}
}
