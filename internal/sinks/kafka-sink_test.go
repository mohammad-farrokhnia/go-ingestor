package sinks

import "testing"

type staticRouter struct {
	overrides map[string]string
}

func (s staticRouter) KafkaTopic(tenantID, defaultTopic string) string {
	if t, ok := s.overrides[tenantID]; ok {
		return t
	}
	return defaultTopic
}

func TestKafkaSink_TopicFor_NoRouter(t *testing.T) {
	ks := &KafkaSink{topic: "events"}
	if got := ks.topicFor("acme"); got != "events" {
		t.Fatalf("without a router, topic = %q, want base topic events", got)
	}
}

func TestKafkaSink_TopicFor_RoutesByTenant(t *testing.T) {
	ks := &KafkaSink{
		topic:  "events",
		router: staticRouter{overrides: map[string]string{"acme": "events.acme"}},
	}

	if got := ks.topicFor("acme"); got != "events.acme" {
		t.Fatalf("routed topic = %q, want events.acme", got)
	}
	if got := ks.topicFor("globex"); got != "events" {
		t.Fatalf("unrouted tenant topic = %q, want base events", got)
	}
}
