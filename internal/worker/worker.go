package worker

import (
	"context"
	"log"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/dlq"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

const maxRetries = 3

func Start(ctx context.Context, numWorkers int, buffer <-chan *pb.IngestRequest, batchSize int, batchTimeoutStr string, sinkList []sinks.Sink, recorder metrics.Recorder, dlq dlq.DeadLetterQueue) {
	timeout, err := time.ParseDuration(batchTimeoutStr)
	if err != nil {
		log.Fatalf("Invalid batch_timeout: %v", err)
	}

	log.Printf("Starting %d workers (BatchSize: %d, Timeout: %s)...", numWorkers, batchSize, timeout)

	for i := 0; i < numWorkers; i++ {
		go runWorker(ctx, i, buffer, batchSize, timeout, sinkList, recorder, dlq)
	}
}

func runWorker(ctx context.Context, id int, buffer <-chan *pb.IngestRequest, batchSize int, timeout time.Duration, sinkList []sinks.Sink, recorder metrics.Recorder, dlq dlq.DeadLetterQueue) {

	batch := make([]*pb.IngestRequest, 0, batchSize)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				log.Printf("[WORKER %d] Shutdown: flushing final batch of %d events", id, len(batch))
				flush(id, batch, sinkList, recorder, dlq)
			}
			log.Printf("[WORKER %d] Stopped", id)
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
		if err := writeWithRetry(ctx, sink, batch, workerID); err != nil {
			log.Printf("[WORKER %d] FAILED writing to %s after %d retries: %v", workerID, sink.Name(), maxRetries, err)

			for _, event := range batch {
				if dlqErr := dlq.Push(ctx, event, sink.Name(), err); dlqErr != nil {
					log.Printf("[WORKER %d] Failed to write to DLQ: %v", workerID, dlqErr)
				}
			}
		}
	}

	if recorder != nil {
		recorder.ObserveBatchFlush(time.Since(start).Seconds())
	}

	log.Printf("[WORKER %d] Flushed batch of %d events", workerID, len(batch))
}

func writeWithRetry(ctx context.Context, sink sinks.Sink, batch []*pb.IngestRequest, maxRetries int) error {
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := sink.Write(ctx, batch)
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt < maxRetries {
			backoff := time.Duration(attempt*attempt) * 100 * time.Millisecond
			log.Printf("[RETRY] %s failed (attempt %d/%d), retrying in %v: %v",
				sink.Name(), attempt, maxRetries, backoff, err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return lastErr
}
