package dlq

import (
	"context"
	"os"
	"sync"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"github.com/segmentio/kafka-go"
)

type DLQType = config.DLQType

const (
	dLQTypeFile  = config.DLQTypeFile
	dLQTypeKafka = config.DLQTypeKafka
)

type DeadLetterQueue interface {
	Push(ctx context.Context, event *pb.IngestRequest, sinkName string, err error) error
	Close() error
	Name() string
}

type FileDLQ struct {
	dir  string
	file *os.File
	mu   sync.Mutex
}

type DLQEntry struct {
	Timestamp string            `json:"timestamp"`
	SinkName  string            `json:"sink_name"`
	Error     string            `json:"error"`
	Event     *pb.IngestRequest `json:"event"`
}

type KafkaDLQ struct {
	writer *kafka.Writer
	topic  string
}
