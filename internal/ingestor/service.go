package ingestor

import (
	"errors"
	"log/slog"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	"github.com/mohammad-farrokhnia/ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/ingestor/internal/tenant"
	"github.com/mohammad-farrokhnia/ingestor/internal/wal"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

var ErrTenantQuotaExceeded = errors.New("tenant quota exceeded")

type Service struct {
	buf      buffer.Buffer
	recorder metrics.Recorder
	wal      wal.WAL
	tracker  *wal.SeqTracker
	tenants  *tenant.Registry
}

func NewService(buf buffer.Buffer, recorder metrics.Recorder, w wal.WAL) *Service {
	if w == nil {
		w = wal.NewNoOpWAL()
	}
	return &Service{
		buf:      buf,
		recorder: recorder,
		wal:      w,
		tracker:  wal.NewSeqTracker(),
		tenants:  tenant.NewRegistry(config.TenancyConfig{}), // disabled by default
	}
}

func (s *Service) SetTenants(r *tenant.Registry) {
	if r == nil {
		r = tenant.NewRegistry(config.TenancyConfig{})
	}
	s.tenants = r
}

func (s *Service) SeqTracker() *wal.SeqTracker {
	return s.tracker
}

func (s *Service) Buf() buffer.Buffer {
	return s.buf
}

func (s *Service) WAL() wal.WAL {
	return s.wal
}

func (s *Service) Close() error {
	return s.buf.Close()
}

func (s *Service) Push(req *pb.IngestRequest) error {
	label := s.tenants.MetricLabel(req.TenantId)

	if s.recorder != nil {
		s.recorder.IncEventsReceived(label)
	}

	if !s.tenants.Allow(req.TenantId) {
		if s.recorder != nil {
			s.recorder.IncEventsDropped(label)
		}
		return ErrTenantQuotaExceeded
	}

	seqNum, err := s.wal.Append(req)
	if err != nil {
		slog.Error("WAL append failed", "err", err)
		if s.recorder != nil {
			s.recorder.IncEventsDropped(label)
		}
		return err
	}
	s.tracker.Store(req.EventId, seqNum)

	if err := s.buf.Push(req); err != nil {
		s.tracker.LoadAndDelete(req.EventId)
		if s.recorder != nil {
			s.recorder.IncEventsDropped(label)
		}
		return err
	}
	if s.recorder != nil {
		s.recorder.SetBufferSize(s.buf.Len())
	}
	return nil
}
