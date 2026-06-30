package dlq

import (
	"context"

	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

type NoOpDLQ struct{}

func NewNoOpDLQ() *NoOpDLQ {
	return &NoOpDLQ{}
}

func (d *NoOpDLQ) Push(ctx context.Context, event *pb.IngestRequest, sinkName string, err error) error {
	return nil
}

func (d *NoOpDLQ) Close() error {
	return nil
}

func (d *NoOpDLQ) Name() string {
	return "NoOpDLQ"
}
