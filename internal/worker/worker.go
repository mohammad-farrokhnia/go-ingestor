package worker

import (
	"context"
	"log"
	"time"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/sinks"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func Start(numWorkers int, buffer <-chan *pb.IngestRequest, batchSize int, batchTimeoutStr string, sinkList []sinks.Sink) {
	timeout, err := time.ParseDuration(batchTimeoutStr)
	if err != nil {
		log.Fatalf("Invalid batch_timeout: %v", err)
	}

	log.Printf("Starting %d workers (BatchSize: %d, Timeout: %s)...", numWorkers, batchSize, timeout)

	for i := 0; i < numWorkers; i++ {
		go runWorker(i, buffer, batchSize, timeout, sinkList)
	}
}

func runWorker(id int, buffer <-chan *pb.IngestRequest, batchSize int, timeout time.Duration, sinkList []sinks.Sink) {

	batch := make([]*pb.IngestRequest, 0, batchSize)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case event := <-buffer:
			batch = append(batch, event)

			if len(batch) >= batchSize {
				flush(id, batch, sinkList)
				batch = make([]*pb.IngestRequest, 0, batchSize)
				ticker.Reset(timeout)
			}

		case <-ticker.C:
			if len(batch) > 0 {
				flush(id, batch, sinkList)
				batch = make([]*pb.IngestRequest, 0, batchSize)
			}
		}
	}
}

func flush(workerID int, batch []*pb.IngestRequest, sinkList []sinks.Sink) {
	ctx := context.Background()

	for _, sink := range sinkList {
		if err := sink.Write(ctx, batch); err != nil {
			log.Printf("[WORKER %d] ERROR writing to %s: %v", workerID, sink.Name(), err)
		}
	}

	log.Printf("[WORKER %d] Flushed batch of %d events", workerID, len(batch))
}
