package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	"github.com/mohammad-farrokhnia/ingestor/internal/apperr"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
	"github.com/segmentio/kafka-go"
)

func newKafkaSink(cfg config.KafkaConfig) (*KafkaSink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are not defined")
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic are not defined")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}
	slog.Info("Kafka sink created", "brokers", cfg.Brokers, "topic", cfg.Topic)

	return &KafkaSink{
		writer: writer,
		topic:  cfg.Topic,
	}, nil
}

func (ks *KafkaSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	if len(batch) == 0 {
		return nil
	}
	messages := make([]kafka.Message, 0, len(batch))
	for _, event := range batch {
		payload, err := json.Marshal(event)
		if err != nil {
			slog.Error("Failed to marshal event", "event_id", event.EventId, "err", err)
			return apperr.NewPermanent(ks.Name(), fmt.Errorf("kafka marshal failed for event %s: %w", event.EventId, err))
		}
		messages = append(messages, kafka.Message{
			Key:   []byte(event.EventId),
			Value: payload,
		})
	}

	if err := ks.writer.WriteMessages(ctx, messages...); err != nil {
		return apperr.NewTransient(ks.Name(), fmt.Errorf("kafka write failed: %w", err))
	}

	slog.Debug("Wrote messages to Kafka", "count", len(messages), "topic", ks.topic)
	return nil
}

func (ks *KafkaSink) Name() string {
	return "KafkaSink"
}

func (ks *KafkaSink) Close() error {
	if ks.writer != nil {
		return ks.writer.Close()
	}
	return nil
}
