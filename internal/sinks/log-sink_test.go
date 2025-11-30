package sinks

import (
	"context"
	"testing"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func TestLogSink_New(t *testing.T) {
	sink, err := newLogSink()

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink == nil {
		t.Fatal("expected sink to be created")
	}
}

func TestLogSink_Name(t *testing.T) {
	sink, _ := newLogSink()

	if sink.Name() != "LogSink" {
		t.Errorf("expected name 'LogSink', got %s", sink.Name())
	}
}

func TestLogSink_Write_EmptyBatch(t *testing.T) {
	sink, _ := newLogSink()
	ctx := context.Background()

	err := sink.Write(ctx, []*pb.IngestRequest{})

	if err != nil {
		t.Fatalf("expected no error for empty batch, got %v", err)
	}
}

func TestLogSink_Write_NilBatch(t *testing.T) {
	sink, _ := newLogSink()
	ctx := context.Background()

	err := sink.Write(ctx, nil)

	if err != nil {
		t.Fatalf("expected no error for nil batch, got %v", err)
	}
}

func TestLogSink_Write_SingleEvent(t *testing.T) {
	sink, _ := newLogSink()
	ctx := context.Background()

	batch := []*pb.IngestRequest{
		{EventId: "event-1", Source: "test", Payload: "{}"},
	}

	err := sink.Write(ctx, batch)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestLogSink_Write_MultipleBatches(t *testing.T) {
	sink, _ := newLogSink()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		batch := []*pb.IngestRequest{
			{EventId: "event", Source: "test"},
		}
		if err := sink.Write(ctx, batch); err != nil {
			t.Fatalf("batch %d failed: %v", i, err)
		}
	}
}

func TestLogSink_Close(t *testing.T) {
	sink, _ := newLogSink()

	err := sink.Close()

	if err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}

func TestLogSink_ImplementsSinkInterface(t *testing.T) {
	sink, _ := newLogSink()

	// Compile-time check that LogSink implements Sink
	var _ Sink = sink
}
