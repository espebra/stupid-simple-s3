package main

import (
	"sync"
	"time"
)

// limiter spreads a rate of units per second over all workers. Each caller
// waits for the units taken before it, then adds its own, so the rate holds
// whatever the units are: operations, or bytes written. A nil limiter does
// not limit.
type limiter struct {
	mu   sync.Mutex
	rate float64
	next time.Time
}

func newLimiter(rate float64) *limiter {
	if rate <= 0 {
		return nil
	}
	return &limiter{rate: rate}
}

// wait takes n units, sleeping until the units taken before are paid for or
// stop is closed. Idle time earns no credit, so a burst after it is at most
// one operation.
func (l *limiter) wait(n float64, stop <-chan struct{}) {
	if l == nil || n <= 0 {
		return
	}
	l.mu.Lock()
	now := time.Now()
	start := l.next
	if start.Before(now) {
		start = now
	}
	l.next = start.Add(time.Duration(n / l.rate * float64(time.Second)))
	l.mu.Unlock()
	if d := start.Sub(now); d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-stop:
		}
	}
}
