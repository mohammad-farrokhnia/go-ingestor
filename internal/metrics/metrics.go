package metrics

func New() Recorder {
	return newPrometheusRecorder()
}
