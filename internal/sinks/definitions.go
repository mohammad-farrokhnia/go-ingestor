package sinks

import (
	"context"
	"net/http"
	"time"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker/v2"
)

type Sink interface {
	Write(ctx context.Context, batch []*pb.IngestRequest) error
	Name() string
	Close() error
}

type TopicRouter interface {
	KafkaTopic(tenantID, defaultTopic string) string
}

type SinkType = config.SinkType

const (
	sinkLog   = config.SinkLog
	sinkKafka = config.SinkKafka
	sinkHTTP  = config.SinkHTTP
)

type KafkaSink struct {
	writer *kafka.Writer
	topic  string
	router TopicRouter
}
type HTTPSink struct {
	client  *http.Client
	url     string
	timeout time.Duration
}

type CircuitBreakerSink struct {
	sink Sink
	cb   *gobreaker.CircuitBreaker[any]
}
