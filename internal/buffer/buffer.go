package buffer

import (
	"errors"
	"fmt"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

var ErrBufferFull = errors.New("buffer is full")

type Buffer interface {
	Push(event *pb.IngestRequest) error
	Chan() <-chan *pb.IngestRequest
	Close() error
	Len() int
}

func New(cfg config.BufferConfig, size int) (Buffer, error) {
	switch cfg.Type {
	case "", "channel":
		return NewChannelBuffer(size), nil
	case "redis":
		return NewRedisBuffer(cfg.Redis, size)
	case "hybrid":
		return NewHybridBuffer(cfg.Hybrid, size)
	default:
		return nil, fmt.Errorf("unknown buffer type: %s", cfg.Type)
	}
}
