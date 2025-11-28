package metrics

type Recorder interface {
	IncEventsReceived()
	IncEventsDropped()
	ObserveBatchFlush(seconds float64)
	SetBufferSize(size int)
}
