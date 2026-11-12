package integration

import (
	"sync"
	"time"
)

// BreakerState este starea circuit breaker-ului.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"    // apelurile trec
	BreakerOpen     BreakerState = "open"      // apelurile sunt refuzate local (fallback)
	BreakerHalfOpen BreakerState = "half-open" // o singura cerere de proba
)

// Breaker este un circuit breaker simplu bazat pe esecuri consecutive.
// Protejeaza atat serviciul (nu il bombardam cand e cazut), cat si clientul
// (nu asteapta deadline dupa deadline cand stim deja ca serviciul nu raspunde).
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state    BreakerState
	failures int
	openedAt time.Time
	trial    bool
}

// NewBreaker: se deschide dupa `threshold` esecuri consecutive si incearca
// din nou dupa `cooldown`.
func NewBreaker(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 5 * time.Second
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: time.Now, state: BreakerClosed}
}

// Allow spune daca o cerere poate fi trimisa.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = BreakerHalfOpen
		b.trial = true
		return true
	case BreakerHalfOpen:
		if b.trial {
			return false // o proba e deja in curs
		}
		b.trial = true
		return true
	}
	return true
}

// Success inregistreaza un raspuns de la serviciu (inclusiv erori de business:
// serviciul functioneaza).
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = BreakerClosed
	b.failures = 0
	b.trial = false
}

// Failure inregistreaza un esec de transport / indisponibilitate.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.trial = false
	if b.state == BreakerHalfOpen || b.failures >= b.threshold {
		b.state = BreakerOpen
		b.openedAt = b.now()
	}
}

// State intoarce starea curenta.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == BreakerOpen && b.now().Sub(b.openedAt) >= b.cooldown {
		return BreakerHalfOpen
	}
	return b.state
}
