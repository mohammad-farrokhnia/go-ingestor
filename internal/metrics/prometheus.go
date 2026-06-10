package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type prometheusRecorder struct {
	eventsReceivedTotal       prometheus.Counter
	eventsDroppedTotal        prometheus.Counter
	batchFlushDurationSeconds prometheus.Histogram
	bufferCurrentSize         prometheus.Gauge
	workerPanicsTotal         prometheus.Counter
}

var (
	promInstance *prometheusRecorder
	promOnce     sync.Once
)

func newPrometheusRecorder() *prometheusRecorder {
	promOnce.Do(func() {
		promInstance = &prometheusRecorder{
			eventsReceivedTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "events_received_total",
				Help: "Total number of events received by the ingestor.",
			}),
			eventsDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "events_dropped_total",
				Help: "Total number of events dropped (buffer full, invalid, etc.).",
			}),
			batchFlushDurationSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
				Name:    "batch_flush_duration_seconds",
				Help:    "Duration of batch flush operations in seconds.",
				Buckets: prometheus.DefBuckets,
			}),
			bufferCurrentSize: prometheus.NewGauge(prometheus.GaugeOpts{
				Name: "buffer_current_size",
				Help: "Current number of events buffered in memory.",
			}),
			workerPanicsTotal: prometheus.NewCounter(prometheus.CounterOpts{
				Name: "worker_panics_total",
				Help: "Total number of worker goroutine panics recovered.",
			}),
		}

		prometheus.MustRegister(
			promInstance.eventsReceivedTotal,
			promInstance.eventsDroppedTotal,
			promInstance.batchFlushDurationSeconds,
			promInstance.bufferCurrentSize,
			promInstance.workerPanicsTotal,
		)
	})

	return promInstance
}

func (r *prometheusRecorder) IncEventsReceived() {
	r.eventsReceivedTotal.Inc()
}

func (r *prometheusRecorder) IncEventsDropped() {
	r.eventsDroppedTotal.Inc()
}

func (r *prometheusRecorder) ObserveBatchFlush(seconds float64) {
	r.batchFlushDurationSeconds.Observe(seconds)
}

func (r *prometheusRecorder) SetBufferSize(size int) {
	r.bufferCurrentSize.Set(float64(size))
}

func (r *prometheusRecorder) IncWorkerPanics() {
    r.workerPanicsTotal.Inc()
}