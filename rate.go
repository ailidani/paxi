package paxi

import (
	"sync"
	"time"
)

// Limiter limits operation rate when used with Wait function
type Limiter struct {
	sync.Mutex
	last     time.Time
	sleep    time.Duration
	interval time.Duration
	slack    time.Duration
}

// NewLimiter creates a new rate limiter, where rate is operations per second
func NewLimiter(rate int) *Limiter {
	return &Limiter{
		interval: time.Second / time.Duration(rate),
		slack:    -10 * time.Second / time.Duration(rate),
	}
}

// Wait blocks for the limit
func (l *Limiter) Wait() {
	l.Lock()
	defer l.Unlock()

	now := time.Now()

	if l.last.IsZero() {
		l.last = now
		return
	}

	l.sleep += l.interval - now.Sub(l.last)

	if l.sleep < l.slack {
		l.sleep = l.slack
	}

	if l.sleep > 0 {
		time.Sleep(l.sleep)
		l.last = now.Add(l.sleep)
		l.sleep = 0
	} else {
		l.last = now
	}
}
