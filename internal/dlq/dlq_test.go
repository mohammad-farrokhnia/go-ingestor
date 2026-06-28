package dlq

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func newTestFileDLQ(t *testing.T) *FileDLQ {
	t.Helper()
	d, err := newFileDLQ(t.TempDir())
	if err != nil {
		t.Fatalf("newFileDLQ: %v", err)
	}
	t.Cleanup(func() {
		_ = d.Close()
	})
	return d
}

func sampleEvent(id string) *pb.IngestRequest {
	return &pb.IngestRequest{EventId: id, Source: "test", Payload: `{"k":"v"}`}
}

// ── FileDLQ ──────────────────────────────────────────────────────────────────

func TestFileDLQ_Name(t *testing.T) {
	d := newTestFileDLQ(t)
	if got := d.Name(); got != "FileDLQ" {
		t.Errorf("Name() = %q, want %q", got, "FileDLQ")
	}
}

func TestFileDLQ_Push_WritesEntry(t *testing.T) {
	d := newTestFileDLQ(t)
	ctx := context.Background()

	if err := d.Push(ctx, sampleEvent("e-1"), "MySink", errors.New("boom")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	entries, err := d.DrainEntries()
	if err != nil {
		t.Fatalf("DrainEntries: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.Event.EventId != "e-1" {
		t.Errorf("EventId = %q, want %q", e.Event.EventId, "e-1")
	}
	if e.SinkName != "MySink" {
		t.Errorf("SinkName = %q, want %q", e.SinkName, "MySink")
	}
	if e.Error != "boom" {
		t.Errorf("Error = %q, want %q", e.Error, "boom")
	}
}

func TestFileDLQ_Push_CancelledContext(t *testing.T) {
	d := newTestFileDLQ(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.Push(ctx, sampleEvent("e-x"), "Sink", errors.New("err"))
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestFileDLQ_Push_Concurrent(t *testing.T) {
	d := newTestFileDLQ(t)
	ctx := context.Background()
	const n = 50

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = d.Push(ctx, sampleEvent("e"), "Sink", errors.New("err"))
			_ = i
		}(i)
	}
	wg.Wait()

	entries, err := d.DrainEntries()
	if err != nil {
		t.Fatalf("DrainEntries: %v", err)
	}
	if len(entries) != n {
		t.Errorf("expected %d entries, got %d", n, len(entries))
	}
}

func TestFileDLQ_DrainEntries_Empty(t *testing.T) {
	d := newTestFileDLQ(t)

	entries, err := d.DrainEntries()
	if err != nil {
		t.Fatalf("DrainEntries on empty dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestFileDLQ_DrainEntries_ArchivesFiles(t *testing.T) {
	dir := t.TempDir()
	d, err := newFileDLQ(dir)
	if err != nil {
		t.Fatalf("newFileDLQ: %v", err)
	}
	defer func() { _ = d.Close() }()

	ctx := context.Background()
	if err := d.Push(ctx, sampleEvent("e-1"), "Sink", errors.New("err")); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if _, err := d.DrainEntries(); err != nil {
		t.Fatalf("DrainEntries: %v", err)
	}

	// Original .jsonl files should now have .replayed suffix.
	replayed, _ := filepath.Glob(filepath.Join(dir, "*.replayed"))
	if len(replayed) == 0 {
		t.Error("expected at least one .replayed archive file, found none")
	}
	// No unarchived .jsonl files with entries should remain (a fresh empty one
	// is created after drain, which is fine but will be empty).
	entries, err := d.DrainEntries()
	if err != nil {
		t.Fatalf("second DrainEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after drain, got %d", len(entries))
	}
}

func TestFileDLQ_Stats_Empty(t *testing.T) {
	d := newTestFileDLQ(t)

	stats, err := d.Stats()
	if err != nil {
		t.Fatalf("Stats on empty dir: %v", err)
	}
	if stats.TotalEntries != 0 {
		t.Errorf("TotalEntries = %d, want 0", stats.TotalEntries)
	}
	if stats.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", stats.TotalBytes)
	}
}

func TestFileDLQ_Stats_ReturnsCorrectCounts(t *testing.T) {
	d := newTestFileDLQ(t)
	ctx := context.Background()

	for range 3 {
		if err := d.Push(ctx, sampleEvent("e"), "Sink", errors.New("err")); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	stats, err := d.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.TotalEntries != 3 {
		t.Errorf("TotalEntries = %d, want 3", stats.TotalEntries)
	}
	if stats.TotalBytes == 0 {
		t.Error("TotalBytes should be > 0")
	}
	if len(stats.Files) == 0 {
		t.Error("Files slice should be non-empty")
	}
}

func TestFileDLQ_Close_Idempotent(t *testing.T) {
	d, err := newFileDLQ(t.TempDir())
	if err != nil {
		t.Fatalf("newFileDLQ: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close should not panic. The underlying os.File returns an error
	// on double-close; we just ensure no panic occurs.
	_ = d.Close()
}

// ── NoOpDLQ ──────────────────────────────────────────────────────────────────

func TestNoOpDLQ_Push_ReturnsNil(t *testing.T) {
	d := NewNoOpDLQ()
	err := d.Push(context.Background(), sampleEvent("e"), "Sink", errors.New("err"))
	if err != nil {
		t.Errorf("Push: expected nil, got %v", err)
	}
}

func TestNoOpDLQ_Close_ReturnsNil(t *testing.T) {
	d := NewNoOpDLQ()
	if err := d.Close(); err != nil {
		t.Errorf("Close: expected nil, got %v", err)
	}
}

func TestNoOpDLQ_Name(t *testing.T) {
	d := NewNoOpDLQ()
	if got := d.Name(); got != "NoOpDLQ" {
		t.Errorf("Name() = %q, want %q", got, "NoOpDLQ")
	}
}

// ── KafkaDLQ ─────────────────────────────────────────────────────────────────

func TestNewKafkaDLQ_MissingBrokers(t *testing.T) {
	_, err := NewKafkaDLQ(config.KafkaDLQConfig{Topic: "dlq"})
	if err == nil {
		t.Fatal("expected error for empty brokers, got nil")
	}
}

func TestNewKafkaDLQ_MissingTopic(t *testing.T) {
	_, err := NewKafkaDLQ(config.KafkaDLQConfig{Brokers: []string{"localhost:9092"}})
	if err == nil {
		t.Fatal("expected error for empty topic, got nil")
	}
}

func TestKafkaDLQ_Name(t *testing.T) {
	d, err := NewKafkaDLQ(config.KafkaDLQConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "dlq",
	})
	if err != nil {
		t.Fatalf("NewKafkaDLQ: %v", err)
	}
	defer func() { _ = d.Close() }()
	if got := d.Name(); got != "KafkaDLQ" {
		t.Errorf("Name() = %q, want %q", got, "KafkaDLQ")
	}
}

func TestKafkaDLQ_DoesNotImplementReplayable(t *testing.T) {
	d, err := NewKafkaDLQ(config.KafkaDLQConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "dlq",
	})
	if err != nil {
		t.Fatalf("NewKafkaDLQ: %v", err)
	}
	defer func() { _ = d.Close() }()

	if _, ok := any(d).(Replayable); ok {
		t.Error("KafkaDLQ must NOT implement Replayable")
	}
}

// ── Factory (NewDLQ) ─────────────────────────────────────────────────────────

func TestNewDLQ_Disabled_ReturnsNoOp(t *testing.T) {
	d, err := NewDLQ(config.DLQConfig{Enabled: false})
	if err != nil {
		t.Fatalf("NewDLQ disabled: %v", err)
	}
	if _, ok := d.(*NoOpDLQ); !ok {
		t.Errorf("expected *NoOpDLQ, got %T", d)
	}
}

func TestNewDLQ_FileType_ValidDir(t *testing.T) {
	d, err := NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    config.DLQTypeFile,
		File:    config.FileDLQConfig{Dir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("NewDLQ file: %v", err)
	}
	defer func() { _ = d.Close() }()
	if _, ok := d.(*FileDLQ); !ok {
		t.Errorf("expected *FileDLQ, got %T", d)
	}
}

func TestNewDLQ_FileType_MissingDir_ReturnsError(t *testing.T) {
	// An empty dir string causes MkdirAll("", ...) which will error on most systems,
	// but to be precise we use a known-unwritable path.
	_, err := NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    config.DLQTypeFile,
		File:    config.FileDLQConfig{Dir: "/proc/nonexistent-dlq-test-dir"},
	})
	if err == nil {
		t.Fatal("expected error for unwritable dir, got nil")
	}
}

func TestNewDLQ_KafkaType_ValidConfig(t *testing.T) {
	d, err := NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    config.DLQTypeKafka,
		Kafka:   config.KafkaDLQConfig{Brokers: []string{"localhost:9092"}, Topic: "dlq"},
	})
	if err != nil {
		t.Fatalf("NewDLQ kafka: %v", err)
	}
	defer func() { _ = d.Close() }()
	if _, ok := d.(*KafkaDLQ); !ok {
		t.Errorf("expected *KafkaDLQ, got %T", d)
	}
}

func TestNewDLQ_KafkaType_MissingBrokers(t *testing.T) {
	_, err := NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    config.DLQTypeKafka,
		Kafka:   config.KafkaDLQConfig{Topic: "dlq"},
	})
	if err == nil {
		t.Fatal("expected error for missing brokers, got nil")
	}
}

func TestNewDLQ_UnknownType_ReturnsError(t *testing.T) {
	_, err := NewDLQ(config.DLQConfig{
		Enabled: true,
		Type:    "s3",
	})
	if err == nil {
		t.Fatal("expected error for unknown DLQ type, got nil")
	}
}
