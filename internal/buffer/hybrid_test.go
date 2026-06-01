package buffer

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func hybridCfg(t *testing.T) config.HybridBufferConfig {
	t.Helper()
	return config.HybridBufferConfig{Dir: filepath.Join(t.TempDir(), "overflow")}
}

func TestHybridBuffer_PushAndReceive(t *testing.T) {
	h, err := NewHybridBuffer(hybridCfg(t), 10)
	if err != nil {
		t.Fatalf("NewHybridBuffer: %v", err)
	}
	defer h.Close() //nolint:errcheck

	events := []*pb.IngestRequest{
		{EventId: "1", Source: "s"},
		{EventId: "2", Source: "s"},
		{EventId: "3", Source: "s"},
	}
	for _, e := range events {
		if err := h.Push(e); err != nil {
			t.Fatalf("Push: %v", err)
		}
	}

	for i, want := range events {
		select {
		case got := <-h.Chan():
			if got.EventId != want.EventId {
				t.Errorf("event %d: got %s, want %s", i, got.EventId, want.EventId)
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for event %d", i)
		}
	}
}

func TestHybridBuffer_OverflowToDisk(t *testing.T) {
	memSize := 3
	h, err := NewHybridBuffer(hybridCfg(t), memSize)
	if err != nil {
		t.Fatalf("NewHybridBuffer: %v", err)
	}
	defer h.Close() //nolint:errcheck

	total := 7
	for i := 0; i < total; i++ {
		if err := h.Push(&pb.IngestRequest{EventId: fmt.Sprintf("evt-%d", i)}); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	if h.dq.Len() == 0 {
		t.Fatal("expected disk overflow to be non-empty")
	}
	if h.Len() != total {
		t.Errorf("Len: got %d, want %d", h.Len(), total)
	}
}

func TestHybridBuffer_PumpDiskToMemory(t *testing.T) {
	memSize := 3
	h, err := NewHybridBuffer(hybridCfg(t), memSize)
	if err != nil {
		t.Fatalf("NewHybridBuffer: %v", err)
	}
	defer h.Close() //nolint:errcheck

	total := 6
	for i := 0; i < total; i++ {
		if err := h.Push(&pb.IngestRequest{EventId: fmt.Sprintf("evt-%d", i)}); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	received := make(map[string]bool)
	timeout := time.After(3 * time.Second)
	for len(received) < total {
		select {
		case e := <-h.Chan():
			received[e.EventId] = true
		case <-timeout:
			t.Fatalf("timeout: only received %d/%d events", len(received), total)
		}
	}

	for i := 0; i < total; i++ {
		id := fmt.Sprintf("evt-%d", i)
		if !received[id] {
			t.Errorf("missing event %s", id)
		}
	}
}

func TestHybridBuffer_CloseAndDrain(t *testing.T) {
	memSize := 5
	h, err := NewHybridBuffer(hybridCfg(t), memSize)
	if err != nil {
		t.Fatalf("NewHybridBuffer: %v", err)
	}

	total := 4
	for i := 0; i < total; i++ {
		if err := h.Push(&pb.IngestRequest{EventId: fmt.Sprintf("evt-%d", i)}); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []string
	for e := range h.Chan() {
		got = append(got, e.EventId)
	}
	if len(got) != total {
		t.Errorf("expected %d events after close, got %d", total, len(got))
	}
}

func TestHybridBuffer_PersistAndRecover(t *testing.T) {
	cfg := hybridCfg(t)
	memSize := 3

	h1, err := NewHybridBuffer(cfg, memSize)
	if err != nil {
		t.Fatalf("NewHybridBuffer (1st): %v", err)
	}
	total := 5
	for i := 0; i < total; i++ {
		if err := h1.Push(&pb.IngestRequest{EventId: fmt.Sprintf("evt-%d", i)}); err != nil {
			t.Fatalf("Push %d: %v", i, err)
		}
	}
	close(h1.quit)
	h1.wg.Wait()
	_ = h1.dq.Close()

	diskPending := h1.dq.Len()
	if diskPending == 0 {
		t.Fatal("expected some events on disk before crash simulation")
	}

	h2, err := NewHybridBuffer(cfg, memSize*2)
	if err != nil {
		t.Fatalf("NewHybridBuffer (2nd): %v", err)
	}
	defer h2.Close() //nolint:errcheck

	if h2.dq.Len() == 0 {
		t.Fatal("disk queue should have entries after restart")
	}

	recovered := 0
	timeout := time.After(3 * time.Second)
	for recovered < diskPending {
		select {
		case <-h2.Chan():
			recovered++
		case <-timeout:
			t.Fatalf("timeout recovering disk events: got %d/%d", recovered, diskPending)
		}
	}
}

func TestDiskQueue_EnqueueDequeue(t *testing.T) {
	dir := t.TempDir()
	q, err := newDiskQueue(dir)
	if err != nil {
		t.Fatalf("newDiskQueue: %v", err)
	}
	defer q.Close() //nolint:errcheck

	events := []*pb.IngestRequest{
		{EventId: "a", Source: "x"},
		{EventId: "b", Source: "y"},
		{EventId: "c", Source: "z"},
	}
	for _, e := range events {
		if err := q.Enqueue(e); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	if q.Len() != 3 {
		t.Fatalf("Len: want 3, got %d", q.Len())
	}

	for i, want := range events {
		got, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue %d: %v", i, err)
		}
		if got == nil || got.EventId != want.EventId {
			t.Errorf("event %d: got %v, want %s", i, got, want.EventId)
		}
	}

	empty, err := q.Dequeue()
	if err != nil || empty != nil {
		t.Errorf("expected empty dequeue, got %v, %v", empty, err)
	}
}

func TestDiskQueue_PersistOffset(t *testing.T) {
	dir := t.TempDir()

	q1, _ := newDiskQueue(dir)
	for i := 0; i < 5; i++ {
		_ = q1.Enqueue(&pb.IngestRequest{EventId: fmt.Sprintf("%d", i)})
	}
	for i := 0; i < 3; i++ {
		_, _ = q1.Dequeue()
	}
	_ = q1.Close()

	q2, err := newDiskQueue(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q2.Close() //nolint:errcheck

	if q2.Len() != 2 {
		t.Errorf("expected 2 remaining, got %d", q2.Len())
	}
	e, _ := q2.Dequeue()
	if e.EventId != "3" {
		t.Errorf("expected evt 3, got %s", e.EventId)
	}
}
