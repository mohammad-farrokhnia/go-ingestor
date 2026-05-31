package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"github.com/segmentio/kafka-go"
)

func NewKafkaDLQ(cfg config.KafkaDLQConfig) (*KafkaDLQ, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka DLQ brokers not configured")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka DLQ topic not configured")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}

	log.Printf("[DLQ] Kafka DLQ created brokers=%v topic=%s", cfg.Brokers, cfg.Topic)

	return &KafkaDLQ{writer: writer, topic: cfg.Topic}, nil
}

func (d *KafkaDLQ) Push(ctx context.Context, event *pb.IngestRequest, sinkName string, sinkErr error) error {
	entry := DLQEntry{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		SinkName:  sinkName,
		Error:     sinkErr.Error(),
		Event:     event,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ entry: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.EventId),
		Value: data,
	}

	if err := d.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka DLQ write failed: %w", err)
	}
	return nil
}

func (d *KafkaDLQ) Close() error {
	if d.writer != nil {
		return d.writer.Close()
	}
	return nil
}

func (d *KafkaDLQ) Name() string { return "KafkaDLQ" }
