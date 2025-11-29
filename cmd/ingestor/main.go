package main

import (
	"fmt"
	"log"
	"net"

	"github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	log.Println("Starting ingestor -> loading configs...")
	cfg, err := config.Load("configs")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting ingestor  on port %d with buffer size %d...",
		cfg.Server.GrpcPort, cfg.Ingestor.BufferSize)
	recorder := metrics.New()
	httpServer, err := server.NewHttpServer(cfg.Server.HttpPort)
	if err != nil {
		log.Fatalf("Failed to create HTTP server: %v", err)
	}
	httpServer.Start()
	coreService := ingestor.NewService(cfg.Ingestor.BufferSize, recorder)

	mySinks, err := sinks.BuildMultiSinks(cfg.Sinks.Active, cfg.Sinks)

	if err != nil {
		log.Fatalf("Failed to build sinks: %v", err)
	}

	worker.Start(
		cfg.Worker.NumWorkers,
		coreService.Buffer,
		cfg.Worker.BatchSize,
		cfg.Worker.BatchTimeout,
		mySinks,
		recorder,
	)

	address := fmt.Sprintf("127.0.0.1:%d", cfg.Server.GrpcPort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	myGrpcHandler := server.NewGrpcServer(coreService, recorder)
	myGrpcHandler.Register(grpcServer)

	log.Printf("gRPC server listening on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
