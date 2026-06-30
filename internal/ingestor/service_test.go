package ingestor

import (
	"errors"
	"testing"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	"github.com/mohammad-farrokhnia/ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/ingestor/internal/tenant"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

func TestNewService(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

	if svc == nil {
		t.Fatal("expected service to be created")
	}
}

func TestNewService_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(5)
	svc := NewService(buf, nil, nil)

	if svc == nil {
		t.Fatal("expected service to be created with nil recorder")
	}
}

func TestService_Push_Success(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

	req := &pb.IngestRequest{
		EventId: "test-1",
		Source:  "test",
		Payload: `{"key": "value"}`,
	}

	err := svc.Push(req)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if recorder.GetEventsReceived() != 1 {
		t.Errorf("expected 1 event received, got %d", recorder.GetEventsReceived())
	}
	if buf.Len() != 1 {
		t.Errorf("expected 1 event in buffer, got %d", buf.Len())
	}
}

func TestService_Push_MultipleEvents(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)

	for i := 0; i < 5; i++ {
		req := &pb.IngestRequest{EventId: "test"}
		if err := svc.Push(req); err != nil {
			t.Fatalf("push %d failed: %v", i, err)
		}
	}

	if recorder.GetEventsReceived() != 5 {
		t.Errorf("expected 5 events received, got %d", recorder.GetEventsReceived())
	}
	if buf.Len() != 5 {
		t.Errorf("expected 5 events in buffer, got %d", buf.Len())
	}
}

func TestService_Push_BufferFull(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(2)
	svc := NewService(buf, recorder, nil)

	_ = svc.Push(&pb.IngestRequest{EventId: "1"})
	_ = svc.Push(&pb.IngestRequest{EventId: "2"})

	err := svc.Push(&pb.IngestRequest{EventId: "3"})

	if err == nil {
		t.Fatal("expected error when buffer full")
	}
	if err.Error() != "buffer is full" {
		t.Errorf("expected 'buffer is full' error, got %v", err)
	}
	if recorder.GetEventsDropped() != 1 {
		t.Errorf("expected 1 event dropped, got %d", recorder.GetEventsDropped())
	}
	if recorder.GetEventsReceived() != 3 {
		t.Errorf("expected 3 events received, got %d", recorder.GetEventsReceived())
	}
}

func TestService_Push_TenantQuotaExceeded(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(100)
	svc := NewService(buf, recorder, nil)
	svc.SetTenants(tenant.NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme", RateLimit: 3}},
	}))

	admitted, rejected := 0, 0
	for i := 0; i < 10; i++ {
		err := svc.Push(&pb.IngestRequest{EventId: "e", TenantId: "acme"})
		switch {
		case err == nil:
			admitted++
		case errors.Is(err, ErrTenantQuotaExceeded):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if admitted != 3 {
		t.Errorf("expected 3 admitted (burst), got %d", admitted)
	}
	if rejected != 7 {
		t.Errorf("expected 7 rejected, got %d", rejected)
	}
	if recorder.GetEventsReceivedByTenant("acme") != 10 {
		t.Errorf("expected 10 received for acme, got %d", recorder.GetEventsReceivedByTenant("acme"))
	}
	if recorder.GetEventsDroppedByTenant("acme") != 7 {
		t.Errorf("expected 7 dropped for acme, got %d", recorder.GetEventsDroppedByTenant("acme"))
	}
	if buf.Len() != 3 {
		t.Errorf("expected 3 events buffered, got %d", buf.Len())
	}
}

func TestService_Push_UnknownTenantLabelledOther(t *testing.T) {
	recorder := metrics.NewMock()
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, recorder, nil)
	svc.SetTenants(tenant.NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme"}},
	}))

	if err := svc.Push(&pb.IngestRequest{EventId: "e", TenantId: "ghost"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if recorder.GetEventsReceivedByTenant(tenant.OtherLabel) != 1 {
		t.Errorf("unknown tenant should be bucketed as %q", tenant.OtherLabel)
	}
}

func TestService_Push_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(10)
	svc := NewService(buf, nil, nil)

	req := &pb.IngestRequest{EventId: "test-1"}
	err := svc.Push(req)

	if err != nil {
		t.Fatalf("expected no error with nil recorder, got %v", err)
	}
	if buf.Len() != 1 {
		t.Errorf("expected 1 event in buffer, got %d", buf.Len())
	}
}

func TestService_Push_BufferFull_NilRecorder(t *testing.T) {
	buf := buffer.NewChannelBuffer(1)
	svc := NewService(buf, nil, nil)

	_ = svc.Push(&pb.IngestRequest{EventId: "1"})
	err := svc.Push(&pb.IngestRequest{EventId: "2"})

	if err == nil {
		t.Fatal("expected error when buffer full")
	}
}
