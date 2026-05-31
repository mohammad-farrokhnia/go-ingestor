package ingestor

import (
	"github.com/mohammad-farrokhnia/go-ingestor/internal/buffer"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Service struct {
	buf      buffer.Buffer
	recorder metrics.Recorder
}

func NewService(buf buffer.Buffer, recorder metrics.Recorder) *Service {
	return &Service{
		buf:      buf,
		recorder: recorder,
	}
}

func (s *Service) Buf() buffer.Buffer {
	return s.buf
}

func (s *Service) Push(req *pb.IngestRequest) error {
	if s.recorder != nil {
		s.recorder.IncEventsReceived()
	}
	if err := s.buf.Push(req); err != nil {
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
