package metrics

type Recorder interface {
	IncEventsReceived(tenant string)
	IncEventsDropped(tenant string)
	ObserveBatchFlush(seconds float64)
	SetBufferSize(size int)
	IncWorkerPanics()
}
