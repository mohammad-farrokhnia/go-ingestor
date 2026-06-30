package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"

	"github.com/mohammad-farrokhnia/ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	pb.UnimplementedIngestorServiceServer
	server        *grpc.Server
	ingestor      *ingestor.Service
	recorder      metrics.Recorder
	listener      net.Listener
	addr          string
	ingestEnabled atomic.Bool
	requireTenant atomic.Bool
}

func NewGrpcServer(port int, svc *ingestor.Service, rec metrics.Recorder, ingestEnabled bool) (*GrpcServer, error) {
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
	s.ingestEnabled.Store(ingestEnabled)
	pb.RegisterIngestorServiceServer(s.server, s)
	return s, nil
}

func (s *GrpcServer) SetIngestEnabled(enabled bool) {
	s.ingestEnabled.Store(enabled)
}

func (s *GrpcServer) SetTenancyRequired(required bool) {
	s.requireTenant.Store(required)
}

func (s *GrpcServer) Start() error {
	slog.Info("gRPC server listening", "addr", s.addr)
	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			slog.Error("gRPC server error", "err", err)
		}
	}()
	return nil
}

func (s *GrpcServer) Stop() {
	slog.Info("Shutting down gRPC server")
	s.server.GracefulStop()
}

func (s *GrpcServer) Addr() string {
	return s.addr
}

func (s *GrpcServer) Ingest(ctx context.Context, req *pb.IngestRequest) (*pb.IngestResponse, error) {
	if !s.ingestEnabled.Load() {
		return &pb.IngestResponse{Status: "DISABLED", Error: "ingest disabled"}, nil
	}
	if req.EventId == "" {
		return &pb.IngestResponse{Status: "ERROR", Error: "missing event_id"}, nil
	}
	if s.requireTenant.Load() && req.TenantId == "" {
		return &pb.IngestResponse{Status: "ERROR", Error: "missing tenant_id"}, nil
	}

	err := s.ingestor.Push(req)
	if err != nil {
		if errors.Is(err, ingestor.ErrTenantQuotaExceeded) {
			return &pb.IngestResponse{Status: "DROPPED", Error: "tenant quota exceeded"}, nil
		}
		return &pb.IngestResponse{Status: "DROPPED", Error: "buffer full"}, nil
	}

	return &pb.IngestResponse{Status: "OK"}, nil
}
