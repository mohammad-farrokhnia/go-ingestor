package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func newFileDLQ(dir string) (*FileDLQ, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	filename := filepath.Join(dir, fmt.Sprintf("dlq-%s.jsonl", time.Now().Format("2006-01-02")))
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file: %w", err)
	}

	slog.Info("DLQ writing to file", "path", filename)

	return &FileDLQ{
		dir:  dir,
		file: file,
	}, nil
}

// Push TODO: handle context
func (d *FileDLQ) Push(ctx context.Context, event *pb.IngestRequest, sinkName string, err error) error {
	entry := DLQEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		SinkName:  sinkName,
		Error:     err.Error(),
		Event:     event,
	}

	data, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		return fmt.Errorf("failed to marshal DLQ entry: %w", jsonErr)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, writeErr := d.file.Write(append(data, '\n')); writeErr != nil {
		return fmt.Errorf("failed to write to DLQ: %w", writeErr)
	}

	return nil
}

func (d *FileDLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}

func (d *FileDLQ) Name() string {
	return "FileDLQ"
}
