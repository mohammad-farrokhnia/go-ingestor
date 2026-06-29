package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/wal"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
)

type application struct {
	cfg          *config.Config
	recorder     metrics.Recorder
	core         *ingestor.Service
	http         *server.HttpServer
	grpc         *server.GrpcServer
	sinks        []sinks.Sink
	dlq          dlq.DeadLetterQueue
	wal          wal.WAL
	batchTimeout time.Duration
	workerWg     *sync.WaitGroup
}

func (a *application) setup() error {
	slog.Info("Loading configuration")
	cfg, err := config.Load("configs")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	a.cfg = cfg

	a.recorder = metrics.New()

	// batch_timeout is validated at config load; parse it here so the worker
	// package receives a ready time.Duration and never parses or exits itself.
	a.batchTimeout, err = time.ParseDuration(cfg.Worker.BatchTimeout)
	if err != nil {
		return fmt.Errorf("parse worker.batch_timeout: %w", err)
	}

	a.wal, err = initWAL(cfg)
	if err != nil {
		return fmt.Errorf("init WAL: %w", err)
	}

	a.core, err = initIngestorService(cfg, a.recorder, a.wal)
	if err != nil {
		return fmt.Errorf("init ingestor service: %w", err)
	}

	a.sinks, err = sinks.BuildMultiSinks(cfg.Sinks.Active, cfg.Sinks)
	if err != nil {
		return fmt.Errorf("build sinks: %w", err)
	}

	a.dlq, err = dlq.NewDLQ(cfg.DLQ)
	if err != nil {
		return fmt.Errorf("init DLQ: %w", err)
	}

	a.http, err = server.NewHttpServer(cfg.Server.HttpPort, a.core, cfg.Server.IngestEnabled)
	if err != nil {
		return fmt.Errorf("init HTTP server: %w", err)
	}
	a.http.SetDLQ(a.dlq)
	a.http.SetTenancyRequired(cfg.Tenancy.Enabled)
	a.http.SetAdminToken(cfg.Server.AdminToken)
	if cfg.Server.AdminToken == "" {
		slog.Warn("Admin endpoints (/admin/dlq/*) are unauthenticated; set server.admin_token to require a bearer token")
	}

	a.grpc, err = server.NewGrpcServer(cfg.Server.GrpcPort, a.core, a.recorder, cfg.Server.IngestEnabled)
	if err != nil {
		return fmt.Errorf("init gRPC server: %w", err)
	}
	a.grpc.SetTenancyRequired(cfg.Tenancy.Enabled)

	return nil
}

func (a *application) run(workerCtx context.Context) {
	slog.Info("Starting go-ingestor", "version", Version)
	slog.Info("Starting ingestor service",
		"buffer_type", a.cfg.Buffer.Type,
		"buffer_size", a.cfg.Ingestor.BufferSize,
		"workers", a.cfg.Worker.NumWorkers,
		"batch_size", a.cfg.Worker.BatchSize,
	)

	go a.startHttpServer()
	go a.startGrpcServer()

	replayWAL(a.wal, a.core)

	a.workerWg = worker.Start(workerCtx, worker.Config{
		NumWorkers:   a.cfg.Worker.NumWorkers,
		BatchSize:    a.cfg.Worker.BatchSize,
		BatchTimeout: a.batchTimeout,
	}, worker.Deps{
		Buffer:   a.core.Buf().Chan(),
		Sinks:    a.sinks,
		Recorder: a.recorder,
		DLQ:      a.dlq,
		WAL:      a.wal,
		Tracker:  a.core.SeqTracker(),
	})

	a.http.SetReady(true)
	slog.Info("Ingestor service ready")
}

func (a *application) startHttpServer() {
	if err := a.http.Start(); err != nil {
		slog.Error("HTTP server error", "err", err)
	}
}

func (a *application) startGrpcServer() {
	if err := a.grpc.Start(); err != nil {
		slog.Error("gRPC server error", "err", err)
	}
}

func initIngestorService(cfg *config.Config, recorder metrics.Recorder, w wal.WAL) (*ingestor.Service, error) {
	buf, err := buffer.New(cfg.Buffer, cfg.Ingestor.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("create buffer: %w", err)
	}
	return ingestor.NewService(buf, recorder, w), nil
}

func initWAL(cfg *config.Config) (wal.WAL, error) {
	if !cfg.WAL.Enabled {
		return wal.NewNoOpWAL(), nil
	}
	fileWAL, err := wal.NewFileWAL(cfg.WAL.Dir)
	if err != nil {
		return nil, err
	}
	slog.Info("WAL enabled", "dir", cfg.WAL.Dir)
	return fileWAL, nil
}

func replayWAL(w wal.WAL, core *ingestor.Service) {
	entries, err := w.Recover()
	if err != nil {
		slog.Error("WAL recovery failed", "err", err)
		os.Exit(1)
	}
	if len(entries) == 0 {
		return
	}

	slog.Info("Replaying WAL entries", "count", len(entries))
	for _, entry := range entries {
		if err := core.Push(entry.Event); err != nil {
			slog.Warn("WAL replay: failed to push event", "event_id", entry.Event.EventId, "err", err)
		}
	}
}
