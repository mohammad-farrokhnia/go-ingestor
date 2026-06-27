package ingestor

import (
	"log/slog"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/wal"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Service struct {
	buf      buffer.Buffer
	recorder metrics.Recorder
	wal      wal.WAL
	tracker  *wal.SeqTracker
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
	}
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
	if s.recorder != nil {
		s.recorder.IncEventsReceived()
	}

	seqNum, err := s.wal.Append(req)
	if err != nil {
		slog.Error("WAL append failed", "err", err)
		if s.recorder != nil {
			s.recorder.IncEventsDropped()
		}
		return err
	}
	s.tracker.Store(req.EventId, seqNum)

	if err := s.buf.Push(req); err != nil {
		s.tracker.LoadAndDelete(req.EventId)
		if s.recorder != nil {
			s.recorder.IncEventsDropped()
		}
		return err
	}
	if s.recorder != nil {
		s.recorder.SetBufferSize(s.buf.Len())
	}
	return nil
}
