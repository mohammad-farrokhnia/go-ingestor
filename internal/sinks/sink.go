package sinks

import (
	"fmt"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
)

func BuildMultiSinks(activeNames []config.SinkType, cfg config.SinksConfig) ([]Sink, error) {
	var result []Sink

	for _, name := range activeNames {
		sink, err := BuildSink(name, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to build sink %q: %w", name, err)
		}
		result = append(result, sink)
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no sinks configured")
	}

	return result, nil
}

func BuildSink(name config.SinkType, cfg config.SinksConfig) (Sink, error) {
	switch name {
	case sinkLog:
		return newLogSink()
	case sinkKafka:
		return newKafkaSink(cfg.Kafka)
	case sinkHTTP:
		// TODO: Task 3 - return NewHTTPSink(cfg.HTTP)
		return nil, fmt.Errorf("http sink not implemented yet")
	default:
		return nil, fmt.Errorf("unknown sink type: %s", name)
	}
}
