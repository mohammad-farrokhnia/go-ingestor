package worker

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/mohammad-farrokhnia/ingestor/internal/apperr"
	"github.com/mohammad-farrokhnia/ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/ingestor/internal/sinks"
	"github.com/mohammad-farrokhnia/ingestor/internal/wal"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

const maxRetries = 3

const drainFlushTimeout = 5 * time.Second

var (
	maxRestarts     = 5
	restartCooldown = 1 * time.Second
)

type Config struct {
	NumWorkers   int
	BatchSize    int
	BatchTimeout time.Duration
}

type Deps struct {
	Buffer   <-chan *pb.IngestRequest
	Sinks    []sinks.Sink
	Recorder metrics.Recorder
	DLQ      dlq.DeadLetterQueue
	WAL      wal.WAL
	Tracker  *wal.SeqTracker
}

func Start(ctx context.Context, cfg Config, deps Deps) *sync.WaitGroup {
	if deps.WAL == nil {
		deps.WAL = wal.NewNoOpWAL()
	}
	if deps.Tracker == nil {
		deps.Tracker = wal.NewSeqTracker()
	}

	slog.Info("Starting workers", "count", cfg.NumWorkers, "batch_size", cfg.BatchSize, "timeout", cfg.BatchTimeout)

	var wg sync.WaitGroup
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorkerWithRestart(ctx, id, cfg, deps)
		}(i)
	}
	return &wg
}

func runWorkerWithRestart(ctx context.Context, id int, cfg Config, deps Deps) {
	for attempt := 0; attempt < maxRestarts; attempt++ {
		clean := runWorkerGuarded(ctx, id, cfg, deps)
		if clean {
			return
		}

		if deps.Recorder != nil {
			deps.Recorder.IncWorkerPanics()
		}

		remaining := maxRestarts - attempt - 1
		if remaining == 0 {
			slog.Error("Worker stopped permanently: exceeded max restarts",
				"worker", id,
				"max_restarts", maxRestarts,
			)
			return
		}

		slog.Warn("Worker will restart after panic cooldown",
			"worker", id,
			"attempt", attempt+1,
			"max_restarts", maxRestarts,
			"remaining_restarts", remaining,
			"cooldown", restartCooldown,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(restartCooldown):
		}

		slog.Info("Restarting worker", "worker", id, "attempt", attempt+1)
	}
}

func runWorkerGuarded(ctx context.Context, id int, cfg Config, deps Deps) (exited bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Worker panic recovered",
				"worker", id,
				"panic", r,
				"stack", string(debug.Stack()),
			)
		}
	}()

	runWorker(ctx, id, cfg, deps)
	return true
}

func runWorker(ctx context.Context, id int, cfg Config, deps Deps) {
	batch := make([]*pb.IngestRequest, 0, cfg.BatchSize)

	ticker := time.NewTicker(cfg.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				slog.Info("Shutdown: flushing final batch", "worker", id, "events", len(batch))
				drainFlush(id, batch, deps)
			}
			slog.Info("Worker stopped", "worker", id)
			return

		case event, ok := <-deps.Buffer:
			if !ok {
				if len(batch) > 0 {
					drainFlush(id, batch, deps)
				}
				return
			}
			batch = append(batch, event)

			if len(batch) >= cfg.BatchSize {
				flush(ctx, id, batch, deps)
				batch = make([]*pb.IngestRequest, 0, cfg.BatchSize)
				ticker.Reset(cfg.BatchTimeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				flush(ctx, id, batch, deps)
				batch = make([]*pb.IngestRequest, 0, cfg.BatchSize)
			}
		}
	}
}

func drainFlush(workerID int, batch []*pb.IngestRequest, deps Deps) {
	ctx, cancel := context.WithTimeout(context.Background(), drainFlushTimeout)
	defer cancel()
	flush(ctx, workerID, batch, deps)
}

func flush(ctx context.Context, workerID int, batch []*pb.IngestRequest, deps Deps) {
	start := time.Now()
	allSucceeded := true
	for _, sink := range deps.Sinks {
		if err := writeWithRetry(ctx, sink, batch); err != nil {
			allSucceeded = false
			slog.Error("Sink write failed, routing to DLQ",
				"worker", workerID,
				"sink", sink.Name(),
				"permanent", apperr.IsPermanent(err),
				"err", err,
			)

			for _, event := range batch {
				if dlqErr := deps.DLQ.Push(ctx, event, sink.Name(), err); dlqErr != nil {
					slog.Error("Failed to write to DLQ", "worker", workerID, "err", dlqErr)
				}
			}
		}
	}

	if allSucceeded {
		for _, event := range batch {
			if seqNum, ok := deps.Tracker.LoadAndDelete(event.EventId); ok {
				if err := deps.WAL.Acknowledge(seqNum); err != nil {
					slog.Error("WAL acknowledge failed", "worker", workerID, "seq", seqNum, "err", err)
				}
			}
		}
	}

	if deps.Recorder != nil {
		deps.Recorder.ObserveBatchFlush(time.Since(start).Seconds())
	}

	slog.Debug("Flushed batch", "worker", workerID, "events", len(batch))
}

func writeWithRetry(ctx context.Context, sink sinks.Sink, batch []*pb.IngestRequest) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := sink.Write(ctx, batch)
		if err == nil {
			return nil
		}
		lastErr = err

		if apperr.IsPermanent(err) {
			slog.Warn("Permanent sink error — skipping retries",
				"sink", sink.Name(),
				"attempt", attempt,
				"err", err,
			)
			return err
		}

		if attempt < maxRetries {
			backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
			slog.Warn("Transient sink error — retrying",
				"sink", sink.Name(),
				"attempt", attempt,
				"max_retries", maxRetries,
				"backoff", backoff,
				"err", err,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}
