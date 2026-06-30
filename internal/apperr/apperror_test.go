package apperr

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsTransient_TransientSinkError(t *testing.T) {
	err := NewTransient("HTTPSink", errors.New("connection reset"))
	if !IsTransient(err) {
		t.Error("expected IsTransient=true for transient SinkError")
	}
	if IsPermanent(err) {
		t.Error("expected IsPermanent=false for transient SinkError")
	}
}

func TestIsTransient_PermanentSinkError(t *testing.T) {
	err := NewPermanent("HTTPSink", errors.New("400 Bad Request"))
	if IsTransient(err) {
		t.Error("expected IsTransient=false for permanent SinkError")
	}
	if !IsPermanent(err) {
		t.Error("expected IsPermanent=true for permanent SinkError")
	}
}

func TestIsTransient_PlainError_DefaultsToTransient(t *testing.T) {
	plain := errors.New("unknown network error")
	if !IsTransient(plain) {
		t.Error("plain error should default to transient")
	}
	if IsPermanent(plain) {
		t.Error("plain error should not be classified as permanent")
	}
}

func TestIsTransient_WrappedSinkError_UnwrapsCorrectly(t *testing.T) {
	inner := NewPermanent("KafkaSink", errors.New("marshal failed"))
	wrapped := fmt.Errorf("outer context: %w", inner)

	if !IsPermanent(wrapped) {
		t.Error("wrapped permanent SinkError should still be classified as permanent")
	}
}

func TestIsTransient_NilError(t *testing.T) {
	if !IsTransient(nil) {
		t.Error("nil error should default to transient")
	}
}

func TestSinkError_ErrorString(t *testing.T) {
	cause := errors.New("timeout after 5s")
	se := NewTransient("HTTPSink", cause)
	if se.Error() != cause.Error() {
		t.Errorf("SinkError.Error() = %q, want %q", se.Error(), cause.Error())
	}
}

func TestSinkError_Unwrap(t *testing.T) {
	cause := errors.New("root cause")
	se := NewPermanent("KafkaSink", cause)
	if !errors.Is(se, cause) {
		t.Error("errors.Is should find the root cause through SinkError.Unwrap()")
	}
}

func TestSinkError_Fields(t *testing.T) {
	se := NewPermanent("HTTPSink", errors.New("400"))
	if se.SinkName != "HTTPSink" {
		t.Errorf("SinkName = %q, want HTTPSink", se.SinkName)
	}
	if se.Class != Permanent {
		t.Errorf("Class = %v, want Permanent", se.Class)
	}
}

func TestClassifyHTTPStatus(t *testing.T) {
	cases := []struct {
		status int
		want   ErrorClass
		label  string
	}{
		{200, Transient, "200 OK"},
		{201, Transient, "201 Created"},
		{301, Transient, "301 Redirect"},
		{400, Permanent, "400 Bad Request"},
		{401, Permanent, "401 Unauthorized"},
		{403, Permanent, "403 Forbidden"},
		{404, Permanent, "404 Not Found"},
		{409, Permanent, "409 Conflict"},
		{413, Permanent, "413 Payload Too Large"},
		{422, Permanent, "422 Unprocessable"},
		{429, Transient, "429 Too Many Requests"},
		{499, Permanent, "499 Client Closed"},
		{500, Transient, "500 Internal Server Error"},
		{502, Transient, "502 Bad Gateway"},
		{503, Transient, "503 Service Unavailable"},
		{504, Transient, "504 Gateway Timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got := ClassifyHTTPStatus(tc.status)
			if got != tc.want {
				t.Errorf("ClassifyHTTPStatus(%d) = %v, want %v", tc.status, got, tc.want)
			}
		})
	}
}
