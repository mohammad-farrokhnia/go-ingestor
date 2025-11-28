package sinks

import (
	"context"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Sink interface {
	Write(ctx context.Context, bathc []*pb.IngestRequest) error
	Name() string
	Close() error
}
