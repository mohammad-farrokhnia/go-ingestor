package dlq

import config "github.com/mohammad-farrokhnia/go-ingestor/configs"

func NewKafkaDLQ(cfg config.KafkaDLQConfig) (*KafkaDLQ, error) {

	return &KafkaDLQ{}, nil
}
