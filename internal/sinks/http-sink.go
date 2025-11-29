package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
)

func newHTTPSink(cfg config.HTTPConfig) (*HTTPSink, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("http sink url not configured")
	}

	var timeout time.Duration
	if cfg.Timeout != "" {
		parsed, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid http timeout: %w", err)
		}
		timeout = parsed
	}

	client := &http.Client{
		Timeout: timeout,
	}

	log.Printf("[HTTPSink] Initialized with url=%s timeout=%s", cfg.URL, timeout)

	return &HTTPSink{
		client:  client,
		url:     cfg.URL,
		timeout: timeout,
	}, nil
}

func (hs *HTTPSink) Write(ctx context.Context, batch []*pb.IngestRequest) error {
	if len(batch) == 0 {
		return nil
	}

	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hs.url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hs.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	// Ingest TODO: handle the error
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("http sink returned status %d", resp.StatusCode)
	}

	log.Printf("[HTTPSink] Sent %d events to %s (status: %d)", len(batch), hs.url, resp.StatusCode)
	return nil
}

func (hs *HTTPSink) Name() string {
	return "HTTPSink"
}

func (hs *HTTPSink) Close() error {
	return nil
}
