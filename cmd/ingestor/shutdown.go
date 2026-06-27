package main

import (
	"context"
	"log/slog"
)

func (a *application) shutdown(ctx context.Context, forceStop context.CancelFunc) {
	a.http.SetReady(false)
	a.http.SetIngestEnabled(false)
	a.grpc.SetIngestEnabled(false)

	a.grpc.Stop()
	if err := a.http.Stop(ctx); err != nil {
		slog.Error("HTTP shutdown error", "err", err)
	}

	if err := a.core.Close(); err != nil {
		slog.Error("Failed to close the core service properly", "err", err)
	}

	timeout := a.cfg.ShutdownTimeout()
	slog.Info("Draining buffer", "timeout", timeout.String())
	drained := make(chan struct{})
	go func() {
		a.workerWg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		slog.Info("All workers drained successfully")
	case <-ctx.Done():
		slog.Warn("Shutdown timeout exceeded; forcing worker stop", "timeout", timeout.String())
		forceStop()
		<-drained
	}

	for _, sink := range a.sinks {
		if err := sink.Close(); err != nil {
			slog.Error("Error closing sink", "sink", sink.Name(), "err", err)
		}
	}

	if err := a.dlq.Close(); err != nil {
		slog.Error("Error closing DLQ", "dlq", a.dlq.Name(), "err", err)
	}

	if err := a.wal.Close(); err != nil {
		slog.Error("Error closing WAL", "err", err)
	}

	slog.Info("Shutdown complete")
}
