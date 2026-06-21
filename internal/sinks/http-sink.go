package sinks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	config "github.com/mohammad-farrokhnia/go-ingestor/configs"
	"github.com/mohammad-farrokhnia/go-ingestor/internal/apperr"
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

	slog.Info("HTTP sink initialized", "url", cfg.URL, "timeout", timeout)

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
		return apperr.NewPermanent(hs.Name(), fmt.Errorf("failed to marshal batch: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hs.url, bytes.NewReader(payload))
	if err != nil {
		return apperr.NewPermanent(hs.Name(), fmt.Errorf("failed to create request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hs.client.Do(req)
	if err != nil {
		return apperr.NewTransient(hs.Name(), fmt.Errorf("http request failed: %w", err))
	}
	defer func() {
		if _, drainErr := io.Copy(io.Discard, resp.Body); drainErr != nil {
			slog.Warn("Failed to drain response body", "err", drainErr)
		}
		if closeErr := resp.Body.Close(); closeErr != nil {
			slog.Warn("Failed to close response body", "err", closeErr)
		}
	}()

	if resp.StatusCode >= 400 {
		cause := fmt.Errorf("http sink returned status %d", resp.StatusCode)
		return &apperr.SinkError{
			Class:    apperr.ClassifyHTTPStatus(resp.StatusCode),
			SinkName: hs.Name(),
			Cause:    cause,
		}
	}

	slog.Debug("Sent events to HTTP sink", "count", len(batch), "url", hs.url, "status", resp.StatusCode)
	return nil
}

func (hs *HTTPSink) Name() string {
	return "HTTPSink"
}

func (hs *HTTPSink) Close() error {
	return nil
}
