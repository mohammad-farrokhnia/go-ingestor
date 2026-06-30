package dlq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

func (d *FileDLQ) Push(ctx context.Context, event *pb.IngestRequest, sinkName string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
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

func (d *FileDLQ) DrainEntries() ([]DLQEntry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.file.Sync(); err != nil {
		return nil, fmt.Errorf("dlq drain: sync: %w", err)
	}

	files, err := filepath.Glob(filepath.Join(d.dir, "dlq-*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("dlq drain: glob: %w", err)
	}

	var entries []DLQEntry
	for _, f := range files {
		fe, err := readDLQFile(f)
		if err != nil {
			return nil, fmt.Errorf("dlq drain: read %s: %w", f, err)
		}
		entries = append(entries, fe...)
	}

	if err := d.file.Close(); err != nil {
		return nil, fmt.Errorf("dlq drain: close before archive: %w", err)
	}
	for _, f := range files {
		if err := os.Rename(f, f+".replayed"); err != nil {
			slog.Warn("DLQ drain: could not archive file", "file", f, "err", err)
		}
	}

	newPath := filepath.Join(d.dir, fmt.Sprintf("dlq-%s.jsonl", time.Now().Format("2006-01-02")))
	newFile, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("dlq drain: reopen after archive: %w", err)
	}
	d.file = newFile

	slog.Info("DLQ drained", "entries", len(entries), "files", len(files))
	return entries, nil
}

func (d *FileDLQ) Stats() (DLQStats, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.file.Sync(); err != nil {
		return DLQStats{}, fmt.Errorf("dlq stats: sync: %w", err)
	}

	files, err := filepath.Glob(filepath.Join(d.dir, "dlq-*.jsonl"))
	if err != nil {
		return DLQStats{}, fmt.Errorf("dlq stats: glob: %w", err)
	}

	stats := DLQStats{Files: []DLQFileInfo{}}
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		entries, err := readDLQFile(f)
		if err != nil {
			continue
		}
		stats.Files = append(stats.Files, DLQFileInfo{
			Name:      filepath.Base(f),
			SizeBytes: info.Size(),
			Entries:   len(entries),
		})
		stats.TotalEntries += len(entries)
		stats.TotalBytes += info.Size()
	}
	return stats, nil
}

func readDLQFile(path string) ([]DLQEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer closeOsFile(f)

	const maxLine = 2 * 1024 * 1024
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, maxLine), maxLine)

	var entries []DLQEntry
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry DLQEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			slog.Warn("DLQ: skipping malformed entry", "file", path, "err", err)
			continue
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func closeOsFile(f *os.File) {
	err := f.Close()
	if err != nil {
		slog.Error("FailedToCloseTheFile error:", "err", err)
	}
}

var _ Replayable = (*FileDLQ)(nil)
