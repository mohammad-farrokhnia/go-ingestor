package logging

import (
	"log/slog"
	"os"
	"strings"
	"sync"
)

var (
	mu      sync.RWMutex
	current = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
)

func Init(level, format string) {
	lvl := parseLevel(level)
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: lvl}
	if strings.EqualFold(format, "text") {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	mu.Lock()
	defer mu.Unlock()
	current = slog.New(handler)
	slog.SetDefault(current)
}

func L() *slog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	return current
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "", "info":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}
