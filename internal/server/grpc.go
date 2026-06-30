package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"

	"github.com/mohammad-farrokhnia/ingestor/internal/i18n"
	"github.com/mohammad-farrokhnia/ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	lang := langFromContext(ctx)

	if !s.ingestEnabled.Load() {
		return reply(lang, "DISABLED", "ingest disabled", i18n.MsgDisabled), nil
	}
	if req.EventId == "" {
		return reply(lang, "ERROR", "missing event_id", i18n.MsgMissingEventID), nil
	}
	if s.requireTenant.Load() && req.TenantId == "" {
		return reply(lang, "ERROR", "missing tenant_id", i18n.MsgMissingTenantID), nil
	}

	err := s.ingestor.Push(req)
	if err != nil {
		if errors.Is(err, ingestor.ErrTenantQuotaExceeded) {
			return reply(lang, "DROPPED", "tenant quota exceeded", i18n.MsgTenantQuota), nil
		}
		return reply(lang, "DROPPED", "buffer full", i18n.MsgDropped), nil
	}

	return reply(lang, "OK", "", i18n.MsgAccepted), nil
}

// reply builds a localized IngestResponse. status and legacyErr preserve the
// original machine-readable fields for backward compatibility, while
// message_code, message, and lang carry the i18n layer (mirroring the HTTP
// response envelope's meta).
func reply(lang i18n.Lang, status, legacyErr string, code i18n.MessageCode) *pb.IngestResponse {
	return &pb.IngestResponse{
		Status:      status,
		Error:       legacyErr,
		MessageCode: string(code),
		Message:     i18n.Translate(lang, code),
		Lang:        string(lang),
	}
}

// langFromContext resolves the response language from the incoming gRPC
// "accept-language" metadata, falling back to English.
func langFromContext(ctx context.Context) i18n.Lang {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return i18n.DetectLang("")
	}
	vals := md.Get("accept-language")
	if len(vals) == 0 {
		return i18n.DetectLang("")
	}
	return i18n.DetectLang(vals[0])
}
