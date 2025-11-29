package sinks

import (
	"context"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type Sink interface {
	Write(ctx context.Context, bathc []*pb.IngestRequest) error
	Name() string
	Close() error
}

type SinkType = config.SinkType

const (
	sinkLog   = config.SinkLog
	sinkKafka = config.SinkKafka
	sinkHTTP  = config.SinkHTTP
)
