package ingestor

import (
	"errors"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Service struct {
	Buffer chan *pb.IngestRequest
}

func NewService(bufferSize int) *Service {
	return &Service{
		Buffer: make(chan *pb.IngestRequest, bufferSize),
	}
}

func (s *Service) Push(req *pb.IngestRequest) error {
	select {
	case s.Buffer <- req:
		return nil
	default:
		return errors.New("buffer is full")
	}
}
