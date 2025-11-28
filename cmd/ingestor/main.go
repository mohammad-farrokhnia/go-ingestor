package main

import (
	"fmt"
	"log"
	"net"

	"github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/server"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load("configs")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting go-ingestor on port %d with buffer size %d...",
		cfg.Server.GrpcPort, cfg.Ingestor.BufferSize)

	coreService := ingestor.NewService(cfg.Ingestor.BufferSize)

	mySinks := []sinks.Sink{
		sinks.NewLogSink(),
	}

	worker.Start(
		cfg.Worker.NumWorkers,
		coreService.Buffer,
		cfg.Worker.BatchSize,
		cfg.Worker.BatchTimeout,
		mySinks,
	)

	address := fmt.Sprintf("127.0.0.1:%d", cfg.Server.GrpcPort)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	myGrpcHandler := server.NewGrpcServer(coreService)
	myGrpcHandler.Register(grpcServer)

	log.Printf("gRPC server listening on %s", address)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
