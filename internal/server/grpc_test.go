package server

import (
	"context"
	"testing"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	"github.com/mohammad-farrokhnia/ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/ingestor/internal/i18n"
	"github.com/mohammad-farrokhnia/ingestor/internal/ingestor"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/ingestor/internal/tenant"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
	"google.golang.org/grpc/metadata"
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
	if resp.MessageCode != "MISSING_TENANT_ID" {
		t.Errorf("expected message_code MISSING_TENANT_ID, got %q", resp.MessageCode)
	}
	if resp.Lang != "en" {
		t.Errorf("expected default lang en, got %q", resp.Lang)
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
	if resp.MessageCode != "ACCEPTED" {
		t.Errorf("expected message_code ACCEPTED, got %q", resp.MessageCode)
	}
	if resp.Message != i18n.Translate(i18n.LangEN, i18n.MsgAccepted) {
		t.Errorf("expected localized accepted message, got %q", resp.Message)
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

func TestGrpcIngest_LocalizedPersian(t *testing.T) {
	s := newTestGrpcServer(10)

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("accept-language", "fa"),
	)

	resp, err := s.Ingest(ctx, &pb.IngestRequest{EventId: "e1"})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if resp.Lang != "fa" {
		t.Errorf("expected lang fa, got %q", resp.Lang)
	}
	if resp.MessageCode != "ACCEPTED" {
		t.Errorf("expected message_code ACCEPTED, got %q", resp.MessageCode)
	}
	if want := i18n.Translate(i18n.LangFA, i18n.MsgAccepted); resp.Message != want {
		t.Errorf("expected Persian message %q, got %q", want, resp.Message)
	}
	// Status stays machine-readable regardless of language.
	if resp.Status != "OK" {
		t.Errorf("expected status OK, got %q", resp.Status)
	}
}

func TestGrpcIngest_QuotaExceeded_Localized(t *testing.T) {
	buf := buffer.NewChannelBuffer(100)
	svc := ingestor.NewService(buf, metrics.NewMock(), nil)
	svc.SetTenants(tenant.NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme", RateLimit: 1}},
	}))
	s := &GrpcServer{ingestor: svc, recorder: metrics.NewMock()}
	s.ingestEnabled.Store(true)
	s.SetTenancyRequired(true)

	// First event admitted, second throttled.
	if _, err := s.Ingest(context.Background(), &pb.IngestRequest{EventId: "e1", TenantId: "acme"}); err != nil {
		t.Fatalf("first Ingest: %v", err)
	}
	resp, err := s.Ingest(context.Background(), &pb.IngestRequest{EventId: "e2", TenantId: "acme"})
	if err != nil {
		t.Fatalf("second Ingest: %v", err)
	}
	if resp.Status != "DROPPED" {
		t.Errorf("expected status DROPPED, got %q", resp.Status)
	}
	if resp.MessageCode != "TENANT_QUOTA_EXCEEDED" {
		t.Errorf("expected message_code TENANT_QUOTA_EXCEEDED, got %q", resp.MessageCode)
	}
}
