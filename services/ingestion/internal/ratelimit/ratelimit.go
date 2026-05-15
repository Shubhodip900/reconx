// Package ratelimit implements per-source token-bucket rate limiting.
// Each source_system gets its own Limiter with a configurable rate (RPS).
// This prevents a single upstream from overwhelming the ingestion pipeline.
package ratelimit

import (
	"sync"

	"golang.org/x/time/rate"
)

// Limiter manages per-source rate limits using the token-bucket algorithm.
type Limiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
	defaults float64 // default RPS for unconfigured sources
}

// New creates a Limiter with the given default RPS and per-source overrides.
// overrides maps source_system name → RPS.
func New(defaultRPS float64, overrides map[string]float64) *Limiter {
	l := &Limiter{
		limiters: make(map[string]*rate.Limiter),
		defaults: defaultRPS,
	}
	// Pre-populate configured overrides.
	for src, rps := range overrides {
		l.limiters[src] = rate.NewLimiter(rate.Limit(rps), burstFor(rps))
	}
	return l
}

// Allow reports whether a request from sourceSystem is within the rate limit.
// If no limiter exists for the source, one is created on first access.
func (l *Limiter) Allow(sourceSystem string) bool {
	l.mu.RLock()
	lim, ok := l.limiters[sourceSystem]
	l.mu.RUnlock()

	if !ok {
		lim = l.getOrCreate(sourceSystem)
	}
	return lim.Allow()
}

// SetLimit updates or creates a rate limit for the given source at runtime.
func (l *Limiter) SetLimit(sourceSystem string, rps float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limiters[sourceSystem] = rate.NewLimiter(rate.Limit(rps), burstFor(rps))
}

// getOrCreate returns an existing limiter or creates one at the default rate.
func (l *Limiter) getOrCreate(sourceSystem string) *rate.Limiter {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Double-check after acquiring write lock.
	if lim, ok := l.limiters[sourceSystem]; ok {
		return lim
	}
	lim := rate.NewLimiter(rate.Limit(l.defaults), burstFor(l.defaults))
	l.limiters[sourceSystem] = lim
	return lim
}

// burstFor returns a sensible burst size for the given RPS.
// Burst is set to max(1, rps*10) to allow short bursts up to 10x RPS.
func burstFor(rps float64) int {
	burst := int(rps * 10)
	if burst < 1 {
		burst = 1
	}
	return burst
}
