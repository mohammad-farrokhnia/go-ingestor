package wal

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

func tempWALDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	return dir
}

func TestFileWAL_AppendAndRecover(t *testing.T) {
	dir := tempWALDir(t)
	w, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}

	events := []*pb.IngestRequest{
		{EventId: "evt-1", Source: "src-1", Payload: "p1"},
		{EventId: "evt-2", Source: "src-2", Payload: "p2"},
		{EventId: "evt-3", Source: "src-3", Payload: "p3"},
	}

	for _, e := range events {
		seq, appendErr := w.Append(e)
		if appendErr != nil {
			t.Fatalf("Append: %v", appendErr)
		}
		if seq == 0 {
			t.Fatal("expected non-zero seq")
		}
	}

	closeFileWalTest(w)

	w2, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer closeFileWalTest(w2)

	entries, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, entry := range entries {
		if entry.Event.EventId != events[i].EventId {
			t.Errorf("entry %d: expected %s, got %s", i, events[i].EventId, entry.Event.EventId)
		}
	}
}

func TestFileWAL_Acknowledge(t *testing.T) {
	dir := tempWALDir(t)
	w, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}

	seq1, _ := w.Append(&pb.IngestRequest{EventId: "evt-1"})
	_, _ = w.Append(&pb.IngestRequest{EventId: "evt-2"})
	seq3, _ := w.Append(&pb.IngestRequest{EventId: "evt-3"})

	if err := w.Acknowledge(seq1); err != nil {
		t.Fatalf("Acknowledge seq1: %v", err)
	}
	if err := w.Acknowledge(seq3); err != nil {
		t.Fatalf("Acknowledge seq3: %v", err)
	}

	closeFileWalTest(w)

	w2, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := w2.Close(); err != nil {
			t.Errorf("w2.Close: %v", err)
		}
	}()

	entries, err := w2.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 unacked entry, got %d", len(entries))
	}
	if entries[0].Event.EventId != "evt-2" {
		t.Errorf("expected evt-2, got %s", entries[0].Event.EventId)
	}
}

func TestFileWAL_SeqContinuity(t *testing.T) {
	dir := tempWALDir(t)
	w, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}

	seq1, _ := w.Append(&pb.IngestRequest{EventId: "1"})
	seq2, _ := w.Append(&pb.IngestRequest{EventId: "2"})
	closeFileWalTest(w)
	w2, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	seq3, _ := w2.Append(&pb.IngestRequest{EventId: "3"})
	closeFileWalTest(w2)

	if seq1 != 1 || seq2 != 2 || seq3 != 3 {
		t.Errorf("expected seqs 1,2,3 got %d,%d,%d", seq1, seq2, seq3)
	}
}

func closeFileWalTest(w *FileWAL) {
	err := w.Close()
	if err != nil {
		return
	}
}

func TestFileWAL_Checkpoint(t *testing.T) {
	dir := tempWALDir(t)
	w, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}

	seq1, _ := w.Append(&pb.IngestRequest{EventId: "evt-1"})
	_, _ = w.Append(&pb.IngestRequest{EventId: "evt-2"})
	seq3, _ := w.Append(&pb.IngestRequest{EventId: "evt-3"})

	if err := w.Acknowledge(seq1); err != nil {
		t.Fatalf("Acknowledge seq1: %v", err)
	}
	if err := w.Acknowledge(seq3); err != nil {
		t.Fatalf("Acknowledge seq3: %v", err)
	}

	if err := w.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	entries, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover after checkpoint: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after checkpoint, got %d", len(entries))
	}
	if entries[0].Event.EventId != "evt-2" {
		t.Errorf("expected evt-2, got %s", entries[0].Event.EventId)
	}

	info, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatalf("stat wal: %v", err)
	}
	if info.Size() == 0 {
		t.Error("WAL file should not be empty (1 entry remains)")
	}

	closeFileWalTest(w)
}

func TestFileWAL_EmptyRecover(t *testing.T) {
	dir := tempWALDir(t)
	w, err := NewFileWAL(dir)
	if err != nil {
		t.Fatalf("NewFileWAL: %v", err)
	}
	defer closeFileWalTest(w)

	entries, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(entries))
	}
}

func TestNoOpWAL(t *testing.T) {
	w := NewNoOpWAL()

	seq, err := w.Append(&pb.IngestRequest{EventId: "test"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq != 1 {
		t.Errorf("expected seq 1, got %d", seq)
	}

	if err := w.Acknowledge(seq); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	entries, err := w.Recover()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if entries != nil {
		t.Errorf("expected nil entries, got %v", entries)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSeqTracker(t *testing.T) {
	tracker := NewSeqTracker()

	tracker.Store("evt-1", 10)
	tracker.Store("evt-2", 20)

	if tracker.Len() != 2 {
		t.Errorf("expected len 2, got %d", tracker.Len())
	}

	seq, ok := tracker.LoadAndDelete("evt-1")
	if !ok || seq != 10 {
		t.Errorf("expected 10/true, got %d/%v", seq, ok)
	}

	if tracker.Len() != 1 {
		t.Errorf("expected len 1, got %d", tracker.Len())
	}

	_, ok = tracker.LoadAndDelete("evt-1")
	if ok {
		t.Error("expected not found for deleted entry")
	}
}
