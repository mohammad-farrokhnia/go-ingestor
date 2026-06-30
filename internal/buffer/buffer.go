package buffer

import (
	"errors"
	"fmt"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
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
	case "", config.ChannelBuffer:
		return NewChannelBuffer(size), nil
	case config.RedisBuffer:
		return NewRedisBuffer(cfg.Redis, size)
	case config.HybridBuffer:
		return NewHybridBuffer(cfg.Hybrid, size)
	default:
		return nil, fmt.Errorf("unknown buffer type: %s", cfg.Type)
	}
}
