package apperr

import "errors"

type ErrorClass int

const (
	Transient ErrorClass = iota
	Permanent
)

type SinkError struct {
	Class    ErrorClass
	SinkName string
	Cause    error
}

func (e *SinkError) Error() string { return e.Cause.Error() }
func (e *SinkError) Unwrap() error { return e.Cause }

func NewTransient(sinkName string, cause error) *SinkError {
	return &SinkError{Class: Transient, SinkName: sinkName, Cause: cause}
}

func NewPermanent(sinkName string, cause error) *SinkError {
	return &SinkError{Class: Permanent, SinkName: sinkName, Cause: cause}
}

func IsTransient(err error) bool {
	var se *SinkError
	if errors.As(err, &se) {
		return se.Class == Transient
	}
	return true
}

func IsPermanent(err error) bool {
	return !IsTransient(err)
}
