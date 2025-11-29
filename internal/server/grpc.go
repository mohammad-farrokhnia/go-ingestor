package server

import (
	"context"
	"log"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	pb.UnimplementedIngestorServiceServer
	ingestor *ingestor.Service
	recorder metrics.Recorder
}

func NewGrpcServer(i *ingestor.Service, r metrics.Recorder) *GrpcServer {
	return &GrpcServer{
		ingestor: i,
		recorder: r,
	}
}

func (s *GrpcServer) Register(grpcServer *grpc.Server) {
	pb.RegisterIngestorServiceServer(grpcServer, s)
}

// Ingest TODO: handle context
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
