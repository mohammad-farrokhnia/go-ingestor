package buffer

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

const pumpInterval = 10 * time.Millisecond

type HybridBuffer struct {
	ch   chan *pb.IngestRequest
	dq   *diskQueue
	quit chan struct{}
	wg   sync.WaitGroup
}

func NewHybridBuffer(cfg config.HybridBufferConfig, memSize int) (*HybridBuffer, error) {
	dq, err := newDiskQueue(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("hybrid buffer disk queue: %w", err)
	}

	h := &HybridBuffer{
		ch:   make(chan *pb.IngestRequest, memSize),
		dq:   dq,
		quit: make(chan struct{}),
	}

	h.wg.Add(1)
	go h.pump()

	slog.Info("Hybrid buffer initialized",
		"mem_size", memSize,
		"overflow_dir", cfg.Dir,
		"overflow_pending", dq.Len(),
	)
	return h, nil
}

func (h *HybridBuffer) Push(event *pb.IngestRequest) error {
	if h.dq.IsEmpty() {
		select {
		case h.ch <- event:
			return nil
		default:
		}
	}
	if err := h.dq.Enqueue(event); err != nil {
		return fmt.Errorf("overflow disk full: %w", err)
	}
	return nil
}

func (h *HybridBuffer) Chan() <-chan *pb.IngestRequest {
	return h.ch
}

func (h *HybridBuffer) Len() int {
	return len(h.ch) + h.dq.Len()
}

func (h *HybridBuffer) Close() error {
	close(h.quit)
	h.wg.Wait()
	return h.dq.Close()
}

func (h *HybridBuffer) pump() {
	defer h.wg.Done()
	ticker := time.NewTicker(pumpInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.quit:
			h.drainOverflow()
			close(h.ch)
			return
		case <-ticker.C:
			h.pumpBatch()
		}
	}
}

func (h *HybridBuffer) pumpBatch() {
	for {
		if h.dq.IsEmpty() {
			return
		}
		if len(h.ch) >= cap(h.ch) {
			return
		}
		event, err := h.dq.Dequeue()
		if err != nil {
			slog.Error("Hybrid buffer: disk dequeue error", "err", err)
			return
		}
		if event == nil {
			return
		}
		select {
		case h.ch <- event:
		case <-h.quit:
			return
		}
	}
}

func (h *HybridBuffer) drainOverflow() {
	for {
		event, err := h.dq.Dequeue()
		if err != nil || event == nil {
			return
		}
		select {
		case h.ch <- event:
		default:
			slog.Warn("Hybrid buffer: channel full during shutdown drain; event remains on disk for next restart",
				"event_id", event.EventId)
			return
		}
	}
}
