package ingestor

import (
	"errors"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Service struct {
	Buffer   chan *pb.IngestRequest
	recorder metrics.Recorder
}

func NewService(bufferSize int, recorder metrics.Recorder) *Service {
	return &Service{
		Buffer:   make(chan *pb.IngestRequest, bufferSize),
		recorder: recorder,
	}
}

func (s *Service) Close() {
	close(s.Buffer)
}

func (s *Service) Push(req *pb.IngestRequest) error {
	if s.recorder != nil {
		s.recorder.IncEventsReceived()
	}
	select {
	case s.Buffer <- req:
		if s.recorder != nil {
			s.recorder.SetBufferSize(len(s.Buffer))
		}
		return nil
	default:
		if s.recorder != nil {
			s.recorder.IncEventsDropped()
		}
		return errors.New("buffer is full")
	}
}
