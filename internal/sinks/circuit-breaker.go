package sinks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"github.com/sony/gobreaker/v2"
)

func newCircuitBreakerSink(sink Sink) *CircuitBreakerSink {
	settings := gobreaker.Settings{
		Name:        sink.Name(),
		MaxRequests: 1,
		Interval:    60 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			slog.Warn("Circuit breaker state change", "name", name, "from", from.String(), "to", to.String())
		},
	}

	return &CircuitBreakerSink{
		sink: sink,
		cb:   gobreaker.NewCircuitBreaker[any](settings),
	}
}

func (c *CircuitBreakerSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	_, err := c.cb.Execute(func() (any, error) {
		return nil, c.sink.Write(ctx, batch)
	})
	if errors.Is(err, gobreaker.ErrOpenState) {
		return fmt.Errorf("circuit breaker open for %s", c.sink.Name())
	}
	return err
}

func (c *CircuitBreakerSink) Name() string {
	return c.sink.Name()
}

func (c *CircuitBreakerSink) Close() error {
	return c.sink.Close()
}
