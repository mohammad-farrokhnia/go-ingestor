package sinks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
	pb "github.com/mohammad-farrokhnia/ingestor/proto/ingestor/v1"
)

func TestHTTPSink_New_MissingURL(t *testing.T) {
	cfg := config.HTTPConfig{URL: ""}

	_, err := newHTTPSink(cfg)

	if err == nil {
		t.Fatal("expected error for missing URL")
	}
}

func TestHTTPSink_New_InvalidTimeout(t *testing.T) {
	cfg := config.HTTPConfig{
		URL:     "http://localhost:8080",
		Timeout: "not-a-duration",
	}

	_, err := newHTTPSink(cfg)

	if err == nil {
		t.Fatal("expected error for invalid timeout")
	}
}

func TestHTTPSink_New_Valid(t *testing.T) {
	cfg := config.HTTPConfig{
		URL:     "http://localhost:8080",
		Timeout: "10s",
	}

	sink, err := newHTTPSink(cfg)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink == nil {
		t.Fatal("expected sink to be created")
	}
}

func TestHTTPSink_New_EmptyTimeout(t *testing.T) {
	cfg := config.HTTPConfig{
		URL:     "http://localhost:8080",
		Timeout: "",
	}

	sink, err := newHTTPSink(cfg)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if sink == nil {
		t.Fatal("expected sink to be created")
	}
}

func TestHTTPSink_Name(t *testing.T) {
	cfg := config.HTTPConfig{URL: "http://localhost:8080"}
	sink, _ := newHTTPSink(cfg)

	if sink.Name() != "HTTPSink" {
		t.Errorf("expected 'HTTPSink', got %s", sink.Name())
	}
}

func TestHTTPSink_Write_EmptyBatch(t *testing.T) {
	cfg := config.HTTPConfig{URL: "http://localhost:8080"}
	sink, _ := newHTTPSink(cfg)
	ctx := context.Background()

	err := sink.Write(ctx, []*pb.IngestRequest{})

	if err != nil {
		t.Fatalf("expected no error for empty batch, got %v", err)
	}
}

func TestHTTPSink_Write_Success(t *testing.T) {
	var receivedBody []byte
	var receivedContentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedContentType = r.Header.Get("Content-Type")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := config.HTTPConfig{URL: server.URL, Timeout: "5s"}
	sink, _ := newHTTPSink(cfg)

	batch := []*pb.IngestRequest{
		{EventId: "event-1", Source: "test", Payload: `{"key":"value"}`},
		{EventId: "event-2", Source: "test", Payload: `{"key":"value2"}`},
	}

	err := sink.Write(context.Background(), batch)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if receivedContentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got %s", receivedContentType)
	}

	var received []map[string]interface{}
	if err := json.Unmarshal(receivedBody, &received); err != nil {
		t.Fatalf("failed to unmarshal received body: %v", err)
	}
	if len(received) != 2 {
		t.Errorf("expected 2 events in payload, got %d", len(received))
	}
}

func TestHTTPSink_Write_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := config.HTTPConfig{URL: server.URL, Timeout: "5s"}
	sink, _ := newHTTPSink(cfg)

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	err := sink.Write(context.Background(), batch)

	if err == nil {
		t.Fatal("expected error for server error response")
	}
}

func TestHTTPSink_Write_ClientError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	cfg := config.HTTPConfig{URL: server.URL, Timeout: "5s"}
	sink, _ := newHTTPSink(cfg)

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	err := sink.Write(context.Background(), batch)

	if err == nil {
		t.Fatal("expected error for client error response")
	}
}

func TestHTTPSink_Write_ConnectionError(t *testing.T) {
	cfg := config.HTTPConfig{URL: "http://localhost:99999", Timeout: "1s"}
	sink, _ := newHTTPSink(cfg)

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	err := sink.Write(context.Background(), batch)

	if err == nil {
		t.Fatal("expected error for connection failure")
	}
}

func TestHTTPSink_Write_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {}
	}))
	defer server.Close()

	cfg := config.HTTPConfig{URL: server.URL, Timeout: "5s"}
	sink, _ := newHTTPSink(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batch := []*pb.IngestRequest{
		{EventId: "event-1"},
	}

	err := sink.Write(ctx, batch)

	if err == nil {
		t.Fatal("expected error for canceled context")
	}
}

func TestHTTPSink_Close(t *testing.T) {
	cfg := config.HTTPConfig{URL: "http://localhost:8080"}
	sink, _ := newHTTPSink(cfg)

	err := sink.Close()

	if err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}
