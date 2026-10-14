// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package proxy

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	// breakerFailureThreshold is the number of consecutive failed requests
	// (network errors or 5xx) that opens the breaker for an instance.
	breakerFailureThreshold = 5
	// breakerInitialBackoff is the first open-state backoff; it doubles on
	// every failed half-open probe up to breakerMaxBackoff.
	breakerInitialBackoff = 5 * time.Second
	breakerMaxBackoff     = 60 * time.Second
	// breakerMaxConcurrent bounds in-flight proxied requests per instance;
	// breakerMaxQueued bounds requests waiting for a slot so backlog cannot
	// grow without limit.
	breakerMaxConcurrent = 16
	breakerMaxQueued     = 32
	// breakerAuthCooldown prevents repeating a failing qBittorrent login in
	// a tight loop when many requests see 403 at once.
	breakerAuthCooldown = 10 * time.Second
	// breakerHalfOpenRetryAfter is advertised while a half-open probe is in
	// flight and all other requests are fast-rejected.
	breakerHalfOpenRetryAfter = time.Second
)

var (
	errBreakerOpen      = errors.New("instance circuit breaker is open")
	errBudgetExhausted  = errors.New("instance request budget exhausted")
	errAuthLoginCooling = errors.New("instance session refresh in cooldown")
)

type breakerState uint8

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

// BreakerConfig configures per-instance circuit breakers. The zero value is
// not valid; use defaultBreakerConfig.
type BreakerConfig struct {
	Enabled          bool
	FailureThreshold int
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	MaxConcurrent    int
	MaxQueued        int
	AuthCooldown     time.Duration
	// now is indirected for tests; nil uses time.Now.
	now func() time.Time
}

func defaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		Enabled:          true,
		FailureThreshold: breakerFailureThreshold,
		InitialBackoff:   breakerInitialBackoff,
		MaxBackoff:       breakerMaxBackoff,
		MaxConcurrent:    breakerMaxConcurrent,
		MaxQueued:        breakerMaxQueued,
		AuthCooldown:     breakerAuthCooldown,
		now:              time.Now,
	}
}

// breakerRejectedError carries the delay the caller should advertise to the
// client. errors.Is matches either errBreakerOpen or errBudgetExhausted.
type breakerRejectedError struct {
	sentinel   error
	retryAfter time.Duration
}

func (e *breakerRejectedError) Error() string { return e.sentinel.Error() }

func (e *breakerRejectedError) Unwrap() error { return e.sentinel }

// authFlight is one single-flighted session refresh shared by every request
// that sees 403 while it runs.
type authFlight struct {
	done chan struct{}
	err  error
}

// instanceBreaker tracks health and budgets for one qBittorrent instance.
type instanceBreaker struct {
	cfg BreakerConfig

	mu                  sync.Mutex
	state               breakerState
	consecutiveFailures int
	totalRequests       int
	totalFailures       int
	lastSuccessAt       time.Time
	lastFailureAt       time.Time
	openUntil           time.Time
	backoff             time.Duration
	halfOpenProbes      int
	authAttempts        int
	authFailures        int
	authCooldownUntil   time.Time
	authInflight        *authFlight

	// sem holds one token per in-flight request; queue bounds waiters.
	sem   chan struct{}
	queue chan struct{}
}

func newInstanceBreaker(cfg BreakerConfig) *instanceBreaker {
	now := cfg.now
	if now == nil {
		now = time.Now
	}
	cfg.now = now
	return &instanceBreaker{
		cfg:   cfg,
		state: breakerClosed,
		sem:   make(chan struct{}, cfg.MaxConcurrent),
		queue: make(chan struct{}, cfg.MaxQueued),
	}
}

func (b *instanceBreaker) now() time.Time {
	return b.cfg.now()
}

// acquire reserves a request slot. The returned release must be called once
// after the request finishes. A non-nil error means the request was not
// admitted: either *breakerRejectedError or the request context error.
func (b *instanceBreaker) acquire(ctx context.Context) (probe bool, release func(), err error) {
	b.mu.Lock()
	switch b.state {
	case breakerClosed:
	case breakerOpen:
		if now := b.now(); !now.Before(b.openUntil) {
			b.state = breakerHalfOpen
			b.halfOpenProbes = 0
		} else {
			retryAfter := b.openUntil.Sub(now)
			b.mu.Unlock()
			return false, nil, &breakerRejectedError{sentinel: errBreakerOpen, retryAfter: retryAfter}
		}
		fallthrough
	case breakerHalfOpen:
		if b.halfOpenProbes > 0 {
			b.mu.Unlock()
			return false, nil, &breakerRejectedError{sentinel: errBreakerOpen, retryAfter: breakerHalfOpenRetryAfter}
		}
		b.halfOpenProbes++
		b.mu.Unlock()
		return true, b.releaseProbe, nil
	}
	b.mu.Unlock()

	// Claim a queue slot first so the number of requests blocked on sem
	// stays bounded.
	select {
	case b.queue <- struct{}{}:
	default:
		return false, nil, &breakerRejectedError{sentinel: errBudgetExhausted}
	}

	select {
	case b.sem <- struct{}{}:
		<-b.queue
		return false, b.releaseSlot, nil
	case <-ctx.Done():
		<-b.queue
		return false, nil, ctx.Err()
	}
}

func (b *instanceBreaker) releaseSlot() {
	<-b.sem
}

func (b *instanceBreaker) releaseProbe() {
	b.mu.Lock()
	if b.halfOpenProbes > 0 {
		b.halfOpenProbes--
	}
	b.mu.Unlock()
}

// record applies the final outcome of one admitted request.
func (b *instanceBreaker) record(probe bool, statusCode int, transportErr error) {
	failed := transportErr != nil || statusCode >= http.StatusInternalServerError

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.totalRequests++

	if !failed {
		b.lastSuccessAt = now
		b.consecutiveFailures = 0
		if b.state == breakerHalfOpen {
			b.state = breakerClosed
			b.backoff = 0
		}
		return
	}

	b.lastFailureAt = now
	b.totalFailures++
	b.consecutiveFailures++

	// Requests admitted as half-open probes reopen immediately on failure.
	// Results arriving while open belong to requests admitted earlier and
	// only update the counters above.
	if b.state == breakerHalfOpen {
		b.openLocked(now, true)
	} else if b.state == breakerClosed && b.consecutiveFailures >= b.cfg.FailureThreshold {
		log.Warn().
			Int("failures", b.consecutiveFailures).
			Msg("Proxy circuit breaker opening after consecutive failures")
		b.openLocked(now, false)
	}
}

// openLocked moves the breaker into the open state. Caller holds b.mu.
func (b *instanceBreaker) openLocked(now time.Time, failedProbe bool) {
	b.state = breakerOpen
	switch {
	case b.backoff <= 0:
		b.backoff = b.cfg.InitialBackoff
	case failedProbe:
		b.backoff *= 2
		if b.backoff > b.cfg.MaxBackoff {
			b.backoff = b.cfg.MaxBackoff
		}
	}
	b.openUntil = now.Add(b.backoff)
}

// refreshSession single-flights qBittorrent logins per instance and enforces a
// cooldown after failures. Requests that find a login in flight wait for its
// result instead of issuing their own. A nil result means the session was
// refreshed and the caller may replay the original request.
func (b *instanceBreaker) refreshSession(ctx context.Context, login func(context.Context) error) error {
	b.mu.Lock()
	if flight := b.authInflight; flight != nil {
		b.mu.Unlock()
		select {
		case <-flight.done:
			return flight.err
		case <-ctx.Done():
			//nolint:wrapcheck // Callers expect unwrapped context errors.
			return ctx.Err()
		}
	}

	if now := b.now(); now.Before(b.authCooldownUntil) {
		b.mu.Unlock()
		return errAuthLoginCooling
	}

	flight := &authFlight{done: make(chan struct{})}
	b.authInflight = flight
	b.mu.Unlock()

	err := login(ctx)

	b.mu.Lock()
	flight.err = err
	b.authInflight = nil
	b.authAttempts++
	if err != nil {
		b.authFailures++
		b.authCooldownUntil = b.now().Add(b.cfg.AuthCooldown)
	} else {
		b.authFailures = 0
	}
	b.mu.Unlock()

	close(flight.done)
	return err
}

// breakerRegistry owns one breaker per qBittorrent instance.
type breakerRegistry struct {
	cfg BreakerConfig

	mu       sync.Mutex
	breakers map[int]*instanceBreaker
}

func newBreakerRegistry(cfg BreakerConfig) *breakerRegistry {
	return &breakerRegistry{
		cfg:      cfg,
		breakers: make(map[int]*instanceBreaker),
	}
}

func (r *breakerRegistry) get(instanceID int) *instanceBreaker {
	r.mu.Lock()
	defer r.mu.Unlock()

	if b, ok := r.breakers[instanceID]; ok {
		return b
	}
	b := newInstanceBreaker(r.cfg)
	r.breakers[instanceID] = b
	return b
}

// retryAfterSeconds converts a backoff into a positive whole-second value for
// the Retry-After header.
func retryAfterSeconds(d time.Duration) int {
	seconds := (d + time.Second - 1) / time.Second
	if seconds < 1 {
		return 1
	}
	return int(seconds)
}

func breakerOpenResponseBody(retryAfter int) []byte {
	return []byte(`{"error":"qBittorrent instance temporarily unavailable, circuit breaker open","retryAfter":` + strconv.Itoa(retryAfter) + `}`)
}

func budgetExhaustedBody() []byte {
	return []byte(`{"error":"qBittorrent instance request budget exhausted, too many requests in flight"}`)
}

func forbiddenCooldownBody() []byte {
	return []byte(`{"error":"qBittorrent session refresh in cooldown"}`)
}
