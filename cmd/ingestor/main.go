package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/logging"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
)

func main() {
	cfg := loadConfig()
	logging.Init(cfg.Logging.Level, cfg.Logging.Format)
	logger := logging.L()

	recorder := metrics.New()
	coreService := ingestor.NewService(cfg.Ingestor.BufferSize, recorder)

	mySinks := initSinks(cfg)

	dlqInstance := initDLQ(cfg)

	logger.Info("Starting ingestor service",
		"buffer_size", cfg.Ingestor.BufferSize,
		"workers", cfg.Worker.NumWorkers,
		"batch_size", cfg.Worker.BatchSize,
	)

	httpServer := initHttpServer(cfg.Server, coreService)

	grpcServer := initGrpcServer(cfg.Server, coreService, recorder)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerWg := worker.Start(ctx, cfg.Worker.NumWorkers, coreService.Buffer, cfg.Worker.BatchSize, cfg.Worker.BatchTimeout, mySinks, recorder, dlqInstance)

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

	logger.Info("Shutdown complete")
}

func initDLQ(cfg *config.Config) dlq.DeadLetterQueue {
	dlqInstance, err := dlq.NewDLQ(cfg.DLQ)
	if err != nil {
		log.Fatalf("Failed to create DLQ: %v", err)
	}
	return dlqInstance
}

func initSinks(cfg *config.Config) []sinks.Sink {
	mySinks, err := sinks.BuildMultiSinks(cfg.Sinks.Active, cfg.Sinks)
	if err != nil {
		log.Fatalf("Failed to build sinks: %v", err)
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
		log.Fatalf("Failed to create HTTP server: %v", err)
	}
	httpServer.Start()
	return httpServer
}

func initGrpcServer(cfg config.ServerConfig, coreService *ingestor.Service, recorder metrics.Recorder) *server.GrpcServer {
	grpcServer, err := server.NewGrpcServer(cfg.GrpcPort, coreService, recorder, cfg.IngestEnabled)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}
	grpcServer.Start()
	return grpcServer
}
