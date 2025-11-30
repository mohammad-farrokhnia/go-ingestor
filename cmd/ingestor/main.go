package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
)

func main() {
	cfg := loadConfig()
	recorder := metrics.New()
	coreService := ingestor.NewService(cfg.Ingestor.BufferSize, recorder)

	mySinks, err := sinks.BuildMultiSinks(cfg.Sinks.Active, cfg.Sinks)
	if err != nil {
		log.Fatalf("Failed to build sinks: %v", err)
	}

	log.Println("Starting ingestor service...")

	httpServer := initHttpServer(cfg.Server)

	grpcServer := initGrpcServer(cfg.Server, coreService, recorder)

	ctx, cancel := context.WithCancel(context.Background())

	worker.Start(ctx, cfg.Worker.NumWorkers, coreService.Buffer, cfg.Worker.BatchSize, cfg.Worker.BatchTimeout, mySinks, recorder)

	httpServer.SetReady(true)
	log.Println("Ingestor service ready")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutdown signal received...")

	httpServer.SetReady(false)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	cancel()
	grpcServer.Stop()
	httpServer.Stop(shutdownCtx)

	for _, sink := range mySinks {
		sink.Close()
	}

	log.Println("Shutdown complete")
}

func loadConfig() *config.Config {
	log.Println("Loading configuration...")
	cfg, err := config.Load("configs")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	return cfg
}

func initHttpServer(cfg config.ServerConfig) *server.HttpServer {
	httpServer, err := server.NewHttpServer(cfg.HttpPort)
	if err != nil {
		log.Fatalf("Failed to create HTTP server: %v", err)
	}
	httpServer.Start()
	return httpServer
}

func initGrpcServer(cfg config.ServerConfig, coreService *ingestor.Service, recorder metrics.Recorder) *server.GrpcServer {
	grpcServer, err := server.NewGrpcServer(cfg.GrpcPort, coreService, recorder)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}
	grpcServer.Start()
	return grpcServer
}
