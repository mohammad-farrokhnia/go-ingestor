package tenant

import (
	"sync"
	"time"
)

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newTokenBucket(ratePerSec float64) *tokenBucket {
	burst := ratePerSec
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{
		rate:   ratePerSec,
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
		now:    time.Now,
	}
}

func (b *tokenBucket) allow() bool {
	if b == nil {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.last = now
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
