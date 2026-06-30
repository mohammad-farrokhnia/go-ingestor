package config

import (
	"strings"
	"testing"
)

func validConfig() *Config {
	return &Config{
		Server: ServerConfig{
			GrpcPort:      50051,
			HttpPort:      8080,
			IngestEnabled: true,
		},
		Ingestor: IngestorConfig{BufferSize: 100},
		Worker: WorkerConfig{
			NumWorkers:   2,
			BatchSize:    10,
			BatchTimeout: "5s",
		},
		Sinks: SinksConfig{
			Active: []SinkType{SinkLog},
		},
		DLQ: DLQConfig{Enabled: false},
	}
}

func TestValidate_Valid(t *testing.T) {
	c := validConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *Config)
		wantSub string
	}{
		{"bad grpc port", func(c *Config) { c.Server.GrpcPort = 0 }, "grpc_port"},
		{"bad http port", func(c *Config) { c.Server.HttpPort = -1 }, "http_port"},
		{"same ports", func(c *Config) { c.Server.HttpPort = c.Server.GrpcPort }, "must differ"},
		{"buffer 0", func(c *Config) { c.Ingestor.BufferSize = 0 }, "buffer_size"},
		{"workers 0", func(c *Config) { c.Worker.NumWorkers = 0 }, "num_workers"},
		{"batch size 0", func(c *Config) { c.Worker.BatchSize = 0 }, "batch_size"},
		{"missing timeout", func(c *Config) { c.Worker.BatchTimeout = "" }, "batch_timeout"},
		{"bad timeout", func(c *Config) { c.Worker.BatchTimeout = "abc" }, "batch_timeout"},
		{"no active sinks", func(c *Config) { c.Sinks.Active = nil }, "sinks.active"},
		{"unknown sink", func(c *Config) { c.Sinks.Active = []SinkType{"bogus"} }, "unknown sink"},
		{"kafka missing brokers", func(c *Config) {
			c.Sinks.Active = []SinkType{SinkKafka}
			c.Sinks.Kafka.Topic = "t"
		}, "kafka.brokers"},
		{"http missing url", func(c *Config) {
			c.Sinks.Active = []SinkType{SinkHTTP}
		}, "http.url"},
		{"dlq file missing dir", func(c *Config) {
			c.DLQ = DLQConfig{Enabled: true, Type: DLQTypeFile}
		}, "dlq.file.dir"},
		{"dlq kafka missing topic", func(c *Config) {
			c.DLQ = DLQConfig{Enabled: true, Type: DLQTypeKafka, Kafka: KafkaDLQConfig{Brokers: []string{"x"}}}
		}, "dlq.kafka.topic"},
		{"dlq unknown type", func(c *Config) {
			c.DLQ = DLQConfig{Enabled: true, Type: "weird"}
		}, "dlq.type"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("expected error to contain %q, got: %v", tc.wantSub, err)
			}
		})
	}
}
