package tenant

import (
	config "github.com/mohammad-farrokhnia/ingestor/configs"
)

const (
	OtherLabel  = "_other"
	SingleLabel = "_single"
)

type Registry struct {
	enabled    bool
	known      map[string]struct{}
	limiters   map[string]*tokenBucket
	defaultLim *tokenBucket
	topics     map[string]string
}

func NewRegistry(cfg config.TenancyConfig) *Registry {
	r := &Registry{
		enabled:  cfg.Enabled,
		known:    make(map[string]struct{}, len(cfg.Tenants)),
		limiters: make(map[string]*tokenBucket, len(cfg.Tenants)),
		topics:   make(map[string]string, len(cfg.Tenants)),
	}

	if cfg.DefaultRateLimit > 0 {
		r.defaultLim = newTokenBucket(float64(cfg.DefaultRateLimit))
	}

	for _, t := range cfg.Tenants {
		if t.ID == "" {
			continue
		}
		r.known[t.ID] = struct{}{}

		rate := t.RateLimit
		if rate == 0 {
			rate = cfg.DefaultRateLimit // inherit default
		}
		if rate > 0 {
			r.limiters[t.ID] = newTokenBucket(float64(rate))
		}

		if t.KafkaTopic != "" {
			r.topics[t.ID] = t.KafkaTopic
		}
	}

	return r
}

func (r *Registry) Enabled() bool {
	return r != nil && r.enabled
}

func (r *Registry) Allow(tenantID string) bool {
	if !r.Enabled() {
		return true
	}
	if _, ok := r.known[tenantID]; ok {
		return r.limiters[tenantID].allow()
	}
	return r.defaultLim.allow()
}

func (r *Registry) MetricLabel(tenantID string) string {
	if !r.Enabled() {
		return SingleLabel
	}
	if _, ok := r.known[tenantID]; ok {
		return tenantID
	}
	return OtherLabel
}

func (r *Registry) KafkaTopic(tenantID, defaultTopic string) string {
	if !r.Enabled() {
		return defaultTopic
	}
	if t, ok := r.topics[tenantID]; ok && t != "" {
		return t
	}
	return defaultTopic
}
