package server

import (
	"context"
	"testing"

	"github.com/mohammad-farrokhnia/ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

func newTestGrpcServer(bufferSize int) *GrpcServer {
	buf := buffer.NewChannelBuffer(bufferSize)
	svc := ingestor.NewService(buf, metrics.NewMock(), nil)
	s := &GrpcServer{ingestor: svc, recorder: metrics.NewMock()}
	s.ingestEnabled.Store(true)
	return s
}

func TestGrpcIngest_TenancyRequired_MissingTenant(t *testing.T) {
	s := newTestGrpcServer(10)
	s.SetTenancyRequired(true)

	resp, err := s.Ingest(context.Background(), &pb.IngestRequest{EventId: "e1"})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if resp.Status != "ERROR" || resp.Error != "missing tenant_id" {
		t.Errorf("expected ERROR/missing tenant_id, got status=%q error=%q", resp.Status, resp.Error)
	}
}

func TestGrpcIngest_TenancyRequired_WithTenant(t *testing.T) {
	s := newTestGrpcServer(10)
	s.SetTenancyRequired(true)

	resp, err := s.Ingest(context.Background(), &pb.IngestRequest{EventId: "e1", TenantId: "acme"})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if resp.Status != "OK" {
		t.Errorf("expected OK, got status=%q error=%q", resp.Status, resp.Error)
	}

	req := <-s.ingestor.Buf().Chan()
	if req.TenantId != "acme" {
		t.Errorf("expected TenantId=acme in buffered event, got %q", req.TenantId)
	}
}

func TestGrpcIngest_TenancyDisabled_TenantOptional(t *testing.T) {
	s := newTestGrpcServer(10) // requireTenant defaults to false

	resp, err := s.Ingest(context.Background(), &pb.IngestRequest{EventId: "e1"})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if resp.Status != "OK" {
		t.Errorf("expected OK when tenancy disabled, got status=%q error=%q", resp.Status, resp.Error)
	}
}
