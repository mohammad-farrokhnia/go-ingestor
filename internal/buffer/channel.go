package buffer

import (
	"sync"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type ChannelBuffer struct {
	ch        chan *pb.IngestRequest
	closeOnce sync.Once
}

func NewChannelBuffer(size int) *ChannelBuffer {
	return &ChannelBuffer{ch: make(chan *pb.IngestRequest, size)}
}

func (b *ChannelBuffer) Push(event *pb.IngestRequest) error {
	select {
	case b.ch <- event:
		return nil
	default:
		return ErrBufferFull
	}
}

func (b *ChannelBuffer) Chan() <-chan *pb.IngestRequest {
	return b.ch
}

func (b *ChannelBuffer) Close() error {
	b.closeOnce.Do(func() {
		close(b.ch)
	})
	return nil
}

func (b *ChannelBuffer) Len() int {
	return len(b.ch)
}
