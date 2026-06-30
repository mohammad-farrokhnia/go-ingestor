package tenant

import (
	"testing"

	config "github.com/mohammad-farrokhnia/ingestor/configs"
)

func TestRegistry_Disabled(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{Enabled: false})

	if r.Enabled() {
		t.Fatal("registry should be disabled")
	}
	if !r.Allow("anything") {
		t.Fatal("disabled registry must admit everything")
	}
	if got := r.MetricLabel("acme"); got != SingleLabel {
		t.Fatalf("disabled label = %q, want %q", got, SingleLabel)
	}
	if got := r.KafkaTopic("acme", "events"); got != "events" {
		t.Fatalf("disabled topic = %q, want default", got)
	}
}

func TestRegistry_NilSafe(t *testing.T) {
	var r *Registry
	if r.Enabled() {
		t.Fatal("nil registry should report disabled")
	}
	if !r.Allow("x") {
		t.Fatal("nil registry should admit")
	}
}

func TestRegistry_MetricLabel_Buckets(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme"}},
	})

	if got := r.MetricLabel("acme"); got != "acme" {
		t.Fatalf("known label = %q, want acme", got)
	}
	if got := r.MetricLabel("unknown"); got != OtherLabel {
		t.Fatalf("unknown label = %q, want %q", got, OtherLabel)
	}
}

func TestRegistry_KafkaTopic_Routing(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{
			{ID: "acme", KafkaTopic: "events.acme"},
			{ID: "globex"},
		},
	})

	if got := r.KafkaTopic("acme", "events"); got != "events.acme" {
		t.Fatalf("acme topic = %q, want events.acme", got)
	}
	if got := r.KafkaTopic("globex", "events"); got != "events" {
		t.Fatalf("globex topic = %q, want default events", got)
	}
	if got := r.KafkaTopic("unknown", "events"); got != "events" {
		t.Fatalf("unknown topic = %q, want default events", got)
	}
}

func TestRegistry_Allow_PerTenantLimit(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme", RateLimit: 3}},
	})

	admitted := 0
	for i := 0; i < 10; i++ {
		if r.Allow("acme") {
			admitted++
		}
	}
	if admitted != 3 {
		t.Fatalf("expected 3 admitted within burst, got %d", admitted)
	}
}

func TestRegistry_Allow_UnlimitedWhenNoLimit(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled: true,
		Tenants: []config.TenantConfig{{ID: "acme"}},
	})

	for i := 0; i < 1000; i++ {
		if !r.Allow("acme") {
			t.Fatal("tenant without a limit must be unlimited")
		}
	}
}

func TestRegistry_Allow_UnknownUsesDefaultLimit(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled:          true,
		DefaultRateLimit: 2,
		Tenants:          []config.TenantConfig{{ID: "acme", RateLimit: 100}},
	})

	admitted := 0
	for i := 0; i < 10; i++ {
		if r.Allow("ghost") {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatalf("unknown tenant admitted %d, want 2 (default limit)", admitted)
	}
}

func TestRegistry_Allow_InheritsDefaultLimit(t *testing.T) {
	r := NewRegistry(config.TenancyConfig{
		Enabled:          true,
		DefaultRateLimit: 4,
		Tenants:          []config.TenantConfig{{ID: "acme"}},
	})

	admitted := 0
	for i := 0; i < 10; i++ {
		if r.Allow("acme") {
			admitted++
		}
	}
	if admitted != 4 {
		t.Fatalf("acme admitted %d, want 4 (inherited default)", admitted)
	}
}
