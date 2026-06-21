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

type DeadLetterQueue interface {
	Push(ctx context.Context, event *pb.IngestRequest, sinkName string, err error) error
	Close() error
	Name() string
}

type Replayable interface {
	DrainEntries() ([]DLQEntry, error)
	Stats() (DLQStats, error)
}

type FileDLQ struct {
	dir  string
	file *os.File
	mu   sync.Mutex
}

type KafkaDLQ struct {
	writer *kafka.Writer
	topic  string
}

type DLQEntry struct {
	Timestamp string            `json:"timestamp"`
	SinkName  string            `json:"sink_name"`
	Error     string            `json:"error"`
	Event     *pb.IngestRequest `json:"event"`
}

type DLQFileInfo struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	Entries   int    `json:"entries"`
}

type DLQStats struct {
	Files        []DLQFileInfo `json:"files"`
	TotalEntries int           `json:"total_entries"`
	TotalBytes   int64         `json:"total_bytes"`
}
