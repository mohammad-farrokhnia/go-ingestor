package sinks

import (
	"context"
	"log/slog"

	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

type LogSink struct{}

func newLogSink() (*LogSink, error) {
	return &LogSink{}, nil
}

func (s *LogSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(batch) == 0 {
		return nil
	}
	slog.Info("Writing batch", "events", len(batch), "first_id", batch[0].EventId)
	return nil
}

func (s *LogSink) Name() string {
	return "LogSink"
}

func (s *LogSink) Close() error {
	return nil
}
