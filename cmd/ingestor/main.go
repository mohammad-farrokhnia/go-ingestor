package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	_ "go.uber.org/automaxprocs"

	"github.com/mohammad-farrokhnia/go-ingestor/internal/response"
)

var Version = "dev"

const appName = "go-ingestor"

// @title           go-ingestor API
// @version         1.0
// @description     HTTP administration and ingestion API for go-ingestor.
// @contact.name    Mohammad Farrokhnia
// @contact.url     https://github.com/mohammad-farrokhnia
// @license.name    MIT
// @license.url     https://opensource.org/licenses/MIT
// @host            localhost:8080
// @BasePath        /
func main() {
	response.Init(appName, Version)

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := &application{}
	if err := app.setup(); err != nil {
		slog.Error("Startup failed", "err", err)
		os.Exit(1)
	}

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()

	app.run(workerCtx)

	<-signalCtx.Done()
	stop()
	slog.Info("Shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), app.cfg.ShutdownTimeout())
	defer cancel()

	app.shutdown(shutdownCtx, stopWorkers)
}
