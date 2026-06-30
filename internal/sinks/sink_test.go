package sinks

import (
	"testing"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
)

func TestBuildSink_Log(t *testing.T) {
	cfg := config.SinksConfig{}

	sink, err := BuildSink(config.SinkLog, cfg)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink == nil {
		t.Fatal("expected sink to be created")
	}
	if sink.Name() != "LogSink" {
		t.Errorf("expected LogSink, got %s", sink.Name())
	}
}

func TestBuildSink_Kafka_MissingBrokers(t *testing.T) {
	cfg := config.SinksConfig{
		Kafka: config.KafkaConfig{
			Brokers: []string{},
			Topic:   "test-topic",
		},
	}

	_, err := BuildSink(config.SinkKafka, cfg)

	if err == nil {
		t.Fatal("expected error for missing brokers")
	}
}

func TestBuildSink_Kafka_MissingTopic(t *testing.T) {
	cfg := config.SinksConfig{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "",
		},
	}

	_, err := BuildSink(config.SinkKafka, cfg)

	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

func TestBuildSink_Kafka_Valid(t *testing.T) {
	cfg := config.SinksConfig{
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test-topic",
		},
	}

	sink, err := BuildSink(config.SinkKafka, cfg)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink.Name() != "KafkaSink" {
		t.Errorf("expected KafkaSink, got %s", sink.Name())
	}
	if err := sink.Close(); err != nil {
		t.Errorf("sink.Close: %v", err)
	}
}

func TestBuildSink_HTTP_MissingURL(t *testing.T) {
	cfg := config.SinksConfig{
		HTTP: config.HTTPConfig{
			URL: "",
		},
	}

	_, err := BuildSink(config.SinkHTTP, cfg)

	if err == nil {
		t.Fatal("expected error for missing URL")
	}
}

func TestBuildSink_HTTP_InvalidTimeout(t *testing.T) {
	cfg := config.SinksConfig{
		HTTP: config.HTTPConfig{
			URL:     "http://localhost:8080",
			Timeout: "invalid",
		},
	}

	_, err := BuildSink(config.SinkHTTP, cfg)

	if err == nil {
		t.Fatal("expected error for invalid timeout")
	}
}

func TestBuildSink_HTTP_Valid(t *testing.T) {
	cfg := config.SinksConfig{
		HTTP: config.HTTPConfig{
			URL:     "http://localhost:8080/webhook",
			Timeout: "5s",
		},
	}

	sink, err := BuildSink(config.SinkHTTP, cfg)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink.Name() != "HTTPSink" {
		t.Errorf("expected HTTPSink, got %s", sink.Name())
	}
}

func TestBuildSink_Unknown(t *testing.T) {
	cfg := config.SinksConfig{}

	_, err := BuildSink(config.SinkType("unknown"), cfg)

	if err == nil {
		t.Fatal("expected error for unknown sink type")
	}
}

func TestBuildMultiSinks_Empty(t *testing.T) {
	cfg := config.SinksConfig{}

	_, err := BuildMultiSinks([]config.SinkType{}, cfg, nil)

	if err == nil {
		t.Fatal("expected error for empty sink list")
	}
}

func TestBuildMultiSinks_Single(t *testing.T) {
	cfg := config.SinksConfig{}

	sinks, err := BuildMultiSinks([]config.SinkType{config.SinkLog}, cfg, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(sinks) != 1 {
		t.Errorf("expected 1 sink, got %d", len(sinks))
	}
}

func TestBuildMultiSinks_Multiple(t *testing.T) {
	cfg := config.SinksConfig{
		HTTP: config.HTTPConfig{
			URL:     "http://localhost:8080",
			Timeout: "5s",
		},
	}

	sinks, err := BuildMultiSinks([]config.SinkType{config.SinkLog, config.SinkHTTP}, cfg, nil)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(sinks) != 2 {
		t.Errorf("expected 2 sinks, got %d", len(sinks))
	}
}

func TestBuildMultiSinks_FailsOnInvalid(t *testing.T) {
	cfg := config.SinksConfig{}

	_, err := BuildMultiSinks([]config.SinkType{config.SinkLog, config.SinkType("invalid")}, cfg, nil)

	if err == nil {
		t.Fatal("expected error when one sink is invalid")
	}
}
