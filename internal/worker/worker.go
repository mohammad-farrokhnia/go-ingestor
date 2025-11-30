package worker

import (
	"context"
	"log"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/metrics"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func Start(ctx context.Context, numWorkers int, buffer <-chan *pb.IngestRequest, batchSize int, batchTimeoutStr string, sinkList []sinks.Sink, recorder metrics.Recorder) {
	timeout, err := time.ParseDuration(batchTimeoutStr)
	if err != nil {
		log.Fatalf("Invalid batch_timeout: %v", err)
	}

	log.Printf("Starting %d workers (BatchSize: %d, Timeout: %s)...", numWorkers, batchSize, timeout)

	for i := 0; i < numWorkers; i++ {
		go runWorker(ctx, i, buffer, batchSize, timeout, sinkList, recorder)
	}
}

func runWorker(ctx context.Context, id int, buffer <-chan *pb.IngestRequest, batchSize int, timeout time.Duration, sinkList []sinks.Sink, recorder metrics.Recorder) {

	batch := make([]*pb.IngestRequest, 0, batchSize)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				log.Printf("[WORKER %d] Shutdown: flushing final batch of %d events", id, len(batch))
				flush(id, batch, sinkList, recorder)
			}
			log.Printf("[WORKER %d] Stopped", id)
			return

		case event, ok := <-buffer:
			if !ok {
				if len(batch) > 0 {
					flush(id, batch, sinkList, recorder)
				}
				return
			}
			batch = append(batch, event)

			if len(batch) >= batchSize {
				flush(id, batch, sinkList, recorder)
				batch = make([]*pb.IngestRequest, 0, batchSize)
				ticker.Reset(timeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				flush(id, batch, sinkList, recorder)
				batch = make([]*pb.IngestRequest, 0, batchSize)
			}
		}
	}
}

func flush(workerID int, batch []*pb.IngestRequest, sinkList []sinks.Sink, recorder metrics.Recorder) {
	ctx := context.Background()
	start := time.Now()
	for _, sink := range sinkList {
		if err := sink.Write(ctx, batch); err != nil {
			log.Printf("[WORKER %d] ERROR writing to %s: %v", workerID, sink.Name(), err)
		}
	}

	if recorder != nil {
		recorder.ObserveBatchFlush(time.Since(start).Seconds())
	}

	log.Printf("[WORKER %d] Flushed batch of %d events", workerID, len(batch))
}
