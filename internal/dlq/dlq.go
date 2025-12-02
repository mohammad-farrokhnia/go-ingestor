package dlq

import (
	"fmt"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
)

func NewDLQ(cfg config.DLQConfig) (DeadLetterQueue, error) {
	if !cfg.Enabled {
		return NewNoOpDLQ(), nil
	}

	switch DLQType(cfg.Type) {
	case dLQTypeFile:
		return newFileDLQ(cfg.File.Dir)
	case dLQTypeKafka:
		return nil, fmt.Errorf("kafka DLQ not implemented yet")
	default:
		return nil, fmt.Errorf("unknown DLQ type: %s", cfg.Type)
	}
}
