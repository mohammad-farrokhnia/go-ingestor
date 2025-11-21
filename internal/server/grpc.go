package server

import (
	"context"
	"log"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	pb.UnimplementedIngestorServiceServer
	ingestor *ingestor.Service
}

func NewGrpcServer(ingestor *ingestor.Service) *GrpcServer {
	return &GrpcServer{
		ingestor: ingestor,
	}
}

func (s *GrpcServer) Register(grpcServer *grpc.Server) {
	pb.RegisterIngestorServiceServer(grpcServer, s)
}

func (s *GrpcServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	if req.EventId == "" {
		return &pb.IngestResponse{Status: "ERROR", Error: "missing event_id"}, nil
	}

	err := s.ingestor.Push(req)
	if err != nil {
		log.Printf("Failed to push event %s: %v", req.EventId, err)
		return &pb.IngestResponse{Status: "DROPPED", Error: "buffer full"}, nil
	}

	return &pb.IngestResponse{Status: "OK"}, nil
}
