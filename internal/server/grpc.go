package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	pb.UnimplementedIngestorServiceServer
	server   *grpc.Server
	ingestor *ingestor.Service
	recorder metrics.Recorder
	listener net.Listener
	addr     string
}

func NewGrpcServer(port int, svc *ingestor.Service, rec metrics.Recorder) (*GrpcServer, error) {
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	s := &GrpcServer{
		server:   grpc.NewServer(),
		listener: lis,
		ingestor: svc,
		recorder: rec,
		addr:     addr,
	}
	pb.RegisterIngestorServiceServer(s.server, s)
	return s, nil
}

func (s *GrpcServer) Start() error {
	log.Printf("gRPC server listening on %s", s.addr)
	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()
	return nil
}

func (s *GrpcServer) Stop() {
	log.Println("Shutting down gRPC server...")
	s.server.GracefulStop()
}

func (s *GrpcServer) Addr() string {
	return s.addr
}

func (s *GrpcServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	if req.EventId == "" {
		return &pb.IngestResponse{Status: "ERROR", Error: "missing event_id"}, nil
	}

	err := s.ingestor.Push(req)
	if err != nil {
		return &pb.IngestResponse{Status: "DROPPED", Error: "buffer full"}, nil
	}

	return &pb.IngestResponse{Status: "OK"}, nil
}
