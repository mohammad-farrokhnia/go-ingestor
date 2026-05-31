package worker

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

const maxRetries = 3

func Start(ctx context.Context, numWorkers int, buffer <-chan *pb.IngestRequest, batchSize int, batchTimeoutStr string, sinkList []sinks.Sink, recorder metrics.Recorder, dlq dlq.DeadLetterQueue) *sync.WaitGroup {
	timeout, err := time.ParseDuration(batchTimeoutStr)
	if err != nil {
		slog.Error("Invalid batch_timeout", "err", err)
		os.Exit(1)
	}

	slog.Info("Starting workers", "count", numWorkers, "batch_size", batchSize, "timeout", timeout)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runWorker(ctx, id, buffer, batchSize, timeout, sinkList, recorder, dlq)
		}(i)
	}
	return &wg
}

func runWorker(ctx context.Context, id int, buffer <-chan *pb.IngestRequest, batchSize int, timeout time.Duration, sinkList []sinks.Sink, recorder metrics.Recorder, dlq dlq.DeadLetterQueue) {

	batch := make([]*pb.IngestRequest, 0, batchSize)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				slog.Info("Shutdown: flushing final batch", "worker", id, "events", len(batch))
				flush(id, batch, sinkList, recorder, dlq)
			}
			slog.Info("Worker stopped", "worker", id)
			return

		case event, ok := <-buffer:
			if !ok {
				if len(batch) > 0 {
					flush(id, batch, sinkList, recorder, dlq)
				}
				return
			}
			batch = append(batch, event)

			if len(batch) >= batchSize {
				flush(id, batch, sinkList, recorder, dlq)
				batch = make([]*pb.IngestRequest, 0, batchSize)
				ticker.Reset(timeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				flush(id, batch, sinkList, recorder, dlq)
				batch = make([]*pb.IngestRequest, 0, batchSize)
			}
		}
	}
}

func flush(workerID int, batch []*pb.IngestRequest, sinkList []sinks.Sink, recorder metrics.Recorder, dlq dlq.DeadLetterQueue) {
	ctx := context.Background()
	start := time.Now()
	for _, sink := range sinkList {
		if err := writeWithRetry(ctx, sink, batch); err != nil {
			slog.Error("Failed writing to sink after retries", "worker", workerID, "sink", sink.Name(), "retries", maxRetries, "err", err)

			for _, event := range batch {
				if dlqErr := dlq.Push(ctx, event, sink.Name(), err); dlqErr != nil {
					slog.Error("Failed to write to DLQ", "worker", workerID, "err", dlqErr)
				}
			}
		}
	}

	if recorder != nil {
		recorder.ObserveBatchFlush(time.Since(start).Seconds())
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

		if attempt < maxRetries {
			backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
			slog.Warn("Sink write failed, retrying", "sink", sink.Name(), "attempt", attempt, "max_retries", maxRetries, "backoff", backoff, "err", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}
