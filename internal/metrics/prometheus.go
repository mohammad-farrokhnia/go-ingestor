package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const tenantLabel = "tenant"

type prometheusRecorder struct {
	eventsReceivedTotal       *prometheus.CounterVec
	eventsDroppedTotal        *prometheus.CounterVec
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
			eventsReceivedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "events_received_total",
				Help: "Total number of events received by the ingestor.",
			}, []string{tenantLabel}),
			eventsDroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "events_dropped_total",
				Help: "Total number of events dropped (buffer full, invalid, quota, etc.).",
			}, []string{tenantLabel}),
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

func (r *prometheusRecorder) IncEventsReceived(tenant string) {
	r.eventsReceivedTotal.WithLabelValues(tenant).Inc()
}

func (r *prometheusRecorder) IncEventsDropped(tenant string) {
	r.eventsDroppedTotal.WithLabelValues(tenant).Inc()
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
