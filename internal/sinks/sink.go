package sinks

import (
	"fmt"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
)

func BuildMultiSinks(activeNames []config.SinkType, cfg config.SinksConfig, router TopicRouter) ([]Sink, error) {
	var result []Sink

	for _, name := range activeNames {
		sink, err := BuildSink(name, cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to build sink %q: %w", name, err)
		}
		if ks, ok := sink.(*KafkaSink); ok {
			ks.router = router
		}
		result = append(result, newCircuitBreakerSink(sink))
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
		return newHTTPSink(cfg.HTTP)
	default:
		return nil, fmt.Errorf("unknown sink type: %s", name)
	}
}
