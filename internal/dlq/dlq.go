package dlq

import (
	"fmt"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
)

func NewDLQ(cfg config.DLQConfig) (DeadLetterQueue, error) {
	if !cfg.Enabled {
		return NewNoOpDLQ(), nil
	}

	switch DLQType(cfg.Type) {
	case config.DLQTypeFile:
		return newFileDLQ(cfg.File.Dir)
	case config.DLQTypeKafka:
		return NewKafkaDLQ(cfg.Kafka)
	default:
		return nil, fmt.Errorf("unknown DLQ type: %s", cfg.Type)
	}
}
