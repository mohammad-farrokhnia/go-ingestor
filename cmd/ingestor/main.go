package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	_ "go.uber.org/automaxprocs"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/logging"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/wal"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
)

func main() {
	cfg := loadConfig()
	logging.Init(cfg.Logging.Level, cfg.Logging.Format)
	logger := logging.L()

	recorder := metrics.New()

	var w wal.WAL
	if cfg.WAL.Enabled {
		fileWAL, walErr := wal.NewFileWAL(cfg.WAL.Dir)
		if walErr != nil {
			slog.Error("Failed to create WAL", "err", walErr)
			os.Exit(1)
		}
		w = fileWAL
		logger.Info("WAL enabled", "dir", cfg.WAL.Dir)
	}

	buf, err := buffer.New(cfg.Buffer, cfg.Ingestor.BufferSize)
	if err != nil {
		slog.Error("Failed to create buffer", "err", err)
		os.Exit(1)
	}

	coreService := ingestor.NewService(buf, recorder, w)

	mySinks := initSinks(cfg)

	dlqInstance := initDLQ(cfg)

	logger.Info("Starting ingestor service",
		"buffer_type", cfg.Buffer.Type,
		"buffer_size", cfg.Ingestor.BufferSize,
		"workers", cfg.Worker.NumWorkers,
		"batch_size", cfg.Worker.BatchSize,
	)

	httpServer := initHttpServer(cfg.Server, coreService)

	grpcServer := initGrpcServer(cfg.Server, coreService, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if w != nil {
		entries, recoverErr := w.Recover()
		if recoverErr != nil {
			slog.Error("WAL recovery failed", "err", recoverErr)
			os.Exit(1)
		}
		if len(entries) > 0 {
			logger.Info("Replaying WAL entries", "count", len(entries))
			for _, entry := range entries {
				if pushErr := coreService.Push(entry.Event); pushErr != nil {
					logger.Warn("WAL replay: failed to push event", "event_id", entry.Event.EventId, "err", pushErr)
				}
			}
		}
	}

	workerWg := worker.Start(ctx, cfg.Worker.NumWorkers, coreService.Buf().Chan(), cfg.Worker.BatchSize, cfg.Worker.BatchTimeout, mySinks, recorder, dlqInstance, w, coreService.SeqTracker())

	httpServer.SetReady(true)
	logger.Info("Ingestor service ready")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Shutdown signal received")

	httpServer.SetReady(false)
	httpServer.SetIngestEnabled(false)
	grpcServer.SetIngestEnabled(false)

	shutdownTimeout := cfg.ShutdownTimeout()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	grpcServer.Stop()
	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", "err", err)
	}

	coreService.Close()

	logger.Info("Draining buffer", "timeout", shutdownTimeout.String())
	drained := make(chan struct{})
	go func() {
		workerWg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		logger.Info("All workers drained successfully")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded; forcing worker stop", "timeout", shutdownTimeout.String())
		cancel()
		<-drained
	}

	for _, sink := range mySinks {
		if err := sink.Close(); err != nil {
			logger.Error("Error closing sink", "sink", sink.Name(), "err", err)
		}
	}

	if err := dlqInstance.Close(); err != nil {
		logger.Error("Error closing DLQ", "dlq", dlqInstance.Name(), "err", err)
	}

	if w != nil {
		if err := w.Close(); err != nil {
			logger.Error("Error closing WAL", "err", err)
		}
	}

	logger.Info("Shutdown complete")
}

func initDLQ(cfg *config.Config) dlq.DeadLetterQueue {
	dlqInstance, err := dlq.NewDLQ(cfg.DLQ)
	if err != nil {
		slog.Error("Failed to create DLQ", "err", err)
		os.Exit(1)
	}
	return dlqInstance
}

func initSinks(cfg *config.Config) []sinks.Sink {
	mySinks, err := sinks.BuildMultiSinks(cfg.Sinks.Active, cfg.Sinks)
	if err != nil {
		slog.Error("Failed to build sinks", "err", err)
		os.Exit(1)
	}
	return mySinks
}

func loadConfig() *config.Config {
	log.Println("Loading configuration...")
	cfg, err := config.Load("configs")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}

func initHttpServer(cfg config.ServerConfig, svc *ingestor.Service) *server.HttpServer {
	httpServer, err := server.NewHttpServer(cfg.HttpPort, svc, cfg.IngestEnabled)
	if err != nil {
		slog.Error("Failed to create HTTP server", "err", err)
		os.Exit(1)
	}
	httpServer.Start()
	return httpServer
}

func initGrpcServer(cfg config.ServerConfig, coreService *ingestor.Service, recorder metrics.Recorder) *server.GrpcServer {
	grpcServer, err := server.NewGrpcServer(cfg.GrpcPort, coreService, recorder, cfg.IngestEnabled)
	if err != nil {
		slog.Error("Failed to create gRPC server", "err", err)
		os.Exit(1)
	}
	grpcServer.Start()
	return grpcServer
}
