package sinks

import (
	"context"
	"log"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

type LogSink struct{}

func newLogSink() *LogSink {
	return &LogSink{}
}

func (s *LogSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	if len(batch) == 0 {
		return nil
	}
	log.Printf("[LogSink] Writing batch of %d events. First ID: %s", len(batch), batch[0].EventId)
	return nil
}

func (s *LogSink) Name() string {
	return "LogSink"
}

func (s *LogSink) Close() error {
	return nil
}
