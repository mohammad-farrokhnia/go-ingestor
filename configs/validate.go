package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

func (c *Config) applyDefaults() {
	if c.Buffer.Type == HybridBuffer && c.Buffer.Hybrid.Dir == "" {
		c.Buffer.Hybrid.Dir = DefaultHybridBufferDir
	}
	if c.WAL.Enabled {
		if c.WAL.Dir == "" {
			c.WAL.Dir = DefaultWALDir
		}
		if c.WAL.CheckpointInterval == "" {
			c.WAL.CheckpointInterval = DefaultCheckpointInterval
		}
	}
}

func (c *Config) Validate() error {
	var errs []string

	c.validateServer(&errs)
	c.validateIngestor(&errs)
	c.validateBuffer(&errs)
	c.validateWorker(&errs)
	c.validateSinks(&errs)
	c.validateDLQ(&errs)
	c.validateWAL(&errs)
	c.validateShutdown(&errs)

	if len(errs) == 0 {
		return nil
	}
	return errors.New("config validation failed:\n  - " + strings.Join(errs, "\n  - "))
}

func (c *Config) validateServer(errs *[]string) {
	if !validPort(c.Server.GrpcPort) {
		*errs = appendErr(*errs, "server.grpc_port", "must be in [1, 65535]")
	}
	if !validPort(c.Server.HttpPort) {
		*errs = appendErr(*errs, "server.http_port", "must be in [1, 65535]")
	}
	if c.Server.GrpcPort == c.Server.HttpPort {
		*errs = appendErr(*errs, "server", "grpc_port and http_port must differ")
	}
}

func (c *Config) validateIngestor(errs *[]string) {
	if c.Ingestor.BufferSize <= 0 {
		*errs = appendErr(*errs, "ingestor.buffer_size", "must be > 0")
	}
}

func (c *Config) validateBuffer(errs *[]string) {
	switch c.Buffer.Type {
	case "", ChannelBuffer, HybridBuffer:
	case RedisBuffer:
		if c.Buffer.Redis.Addr == "" {
			*errs = appendErr(*errs, "buffer.redis.addr", "must be set when buffer type is redis")
		}
	default:
		*errs = appendErr(*errs, "buffer.type", fmt.Sprintf("unknown buffer type %q (must be channel, redis, or hybrid)", c.Buffer.Type))
	}
}

func (c *Config) validateWorker(errs *[]string) {
	if c.Worker.NumWorkers <= 0 {
		*errs = appendErr(*errs, "worker.num_workers", "must be > 0")
	}
	if c.Worker.BatchSize <= 0 {
		*errs = appendErr(*errs, "worker.batch_size", "must be > 0")
	}
	if c.Worker.BatchTimeout == "" {
		*errs = appendErr(*errs, "worker.batch_timeout", "must be set (e.g. \"5s\")")
		return
	}
	if d, err := time.ParseDuration(c.Worker.BatchTimeout); err != nil {
		*errs = appendErr(*errs, "worker.batch_timeout", fmt.Sprintf("invalid duration: %v", err))
	} else if d <= 0 {
		*errs = appendErr(*errs, "worker.batch_timeout", "must be > 0")
	}
}

func (c *Config) validateSinks(errs *[]string) {
	if len(c.Sinks.Active) == 0 {
		*errs = appendErr(*errs, "sinks.active", "at least one sink must be configured")
	}
	for _, name := range c.Sinks.Active {
		switch name {
		case SinkLog:
		case SinkKafka:
			if len(c.Sinks.Kafka.Brokers) == 0 {
				*errs = appendErr(*errs, "sinks.kafka.brokers", "must be set when kafka sink is active")
			}
			if c.Sinks.Kafka.Topic == "" {
				*errs = appendErr(*errs, "sinks.kafka.topic", "must be set when kafka sink is active")
			}
		case SinkHTTP:
			if c.Sinks.HTTP.URL == "" {
				*errs = appendErr(*errs, "sinks.http.url", "must be set when http sink is active")
			}
			if c.Sinks.HTTP.Timeout != "" {
				if _, err := time.ParseDuration(c.Sinks.HTTP.Timeout); err != nil {
					*errs = appendErr(*errs, "sinks.http.timeout", fmt.Sprintf("invalid duration: %v", err))
				}
			}
		default:
			*errs = appendErr(*errs, "sinks.active", fmt.Sprintf("unknown sink type %q", name))
		}
	}
}

func (c *Config) validateDLQ(errs *[]string) {
	if !c.DLQ.Enabled {
		return
	}
	switch DLQType(c.DLQ.Type) {
	case DLQTypeFile:
		if c.DLQ.File.Dir == "" {
			*errs = appendErr(*errs, "dlq.file.dir", "must be set when file DLQ is enabled")
		}
	case DLQTypeKafka:
		if len(c.DLQ.Kafka.Brokers) == 0 {
			*errs = appendErr(*errs, "dlq.kafka.brokers", "must be set when kafka DLQ is enabled")
		}
		if c.DLQ.Kafka.Topic == "" {
			*errs = appendErr(*errs, "dlq.kafka.topic", "must be set when kafka DLQ is enabled")
		}
	default:
		*errs = appendErr(*errs, "dlq.type", fmt.Sprintf("unknown DLQ type %q", c.DLQ.Type))
	}
}

func (c *Config) validateWAL(errs *[]string) {
	if !c.WAL.Enabled || c.WAL.CheckpointInterval == "" {
		return
	}
	if d, err := time.ParseDuration(c.WAL.CheckpointInterval); err != nil {
		*errs = appendErr(*errs, "wal.checkpoint_interval", fmt.Sprintf("invalid duration: %v", err))
	} else if d <= 0 {
		*errs = appendErr(*errs, "wal.checkpoint_interval", "must be > 0")
	}
}

func (c *Config) validateShutdown(errs *[]string) {
	if c.Shutdown.Timeout == "" {
		return
	}
	if d, err := time.ParseDuration(c.Shutdown.Timeout); err != nil {
		*errs = appendErr(*errs, "shutdown.timeout", fmt.Sprintf("invalid duration: %v", err))
	} else if d <= 0 {
		*errs = appendErr(*errs, "shutdown.timeout", "must be > 0")
	}
}

const DefaultShutdownTimeout = 30 * time.Second

func (c *Config) ShutdownTimeout() time.Duration {
	if c.Shutdown.Timeout == "" {
		return DefaultShutdownTimeout
	}
	d, err := time.ParseDuration(c.Shutdown.Timeout)
	if err != nil || d <= 0 {
		return DefaultShutdownTimeout
	}
	return d
}

func validPort(p int) bool { return p > 0 && p <= 65535 }

func appendErr(errs []string, key, msg string) []string {
	return append(errs, fmt.Sprintf("%s: %s", key, msg))
}
