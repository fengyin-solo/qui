// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// scriptedRoundTripper is a concurrency-safe mock whose response depends on
// the per-instance call number.
type scriptedRoundTripper struct {
	mu sync.Mutex
	// calls counts RoundTrip invocations.
	calls int
	// script receives the 1-based call number and returns the outcome.
	script func(call int) (*http.Response, error)
}

func (m *scriptedRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.calls++
	call := m.calls
	script := m.script
	m.mu.Unlock()
	return script(call)
}

func (m *scriptedRoundTripper) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func testBreakerConfig(clock *fakeClock) BreakerConfig {
	return BreakerConfig{
		Enabled:          true,
		FailureThreshold: 3,
		InitialBackoff:   5 * time.Second,
		MaxBackoff:       60 * time.Second,
		MaxConcurrent:    4,
		MaxQueued:        2,
		AuthCooldown:     10 * time.Second,
		now:              clock.now,
	}
}

func jsonResponse(statusCode int) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(http.StatusText(statusCode))),
	}
}

func newBreakerTransport(t *testing.T, rt http.RoundTripper, cfg BreakerConfig) (*RetryTransport, *breakerRegistry) {
	t.Helper()

	registry := newBreakerRegistry(cfg)
	transport := NewRetryTransportWithBreakers(rt, func(_ *http.Request) http.RoundTripper {
		return rt
	}, registry)
	return transport, registry
}

func requestWithProxyContext(pc *proxyContext) *http.Request {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://proxy.example/api/v2/torrents/info", http.NoBody)
	if err != nil {
		panic(err)
	}
	return req.WithContext(context.WithValue(req.Context(), proxyContextKey, pc))
}

func testProxyContext(session sessionRefresher) *proxyContext {
	instanceURL, err := url.Parse("http://qbittorrent.example")
	if err != nil {
		panic(err)
	}
	return &proxyContext{
		instanceID:  1,
		instanceURL: instanceURL,
		httpClient:  &http.Client{},
		session:     session,
	}
}

func TestInstanceBreaker_OpensAfterConsecutiveFailuresAndServes502(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	rt := &scriptedRoundTripper{
		script: func(_ int) (*http.Response, error) {
			// Generic errors are not retried per-request, so each RoundTrip
			// is one upstream call.
			return nil, errors.New("instance down")
		},
	}
	pc := testProxyContext(nil)
	transport, registry := newBreakerTransport(t, rt, cfg)
	breaker := registry.get(1)

	// FailureThreshold-1 failures keep the breaker closed.
	for range cfg.FailureThreshold - 1 {
		resp, err := transport.RoundTrip(requestWithProxyContext(pc))
		if resp != nil {
			_ = resp.Body.Close()
		}
		require.Error(t, err)
	}
	breaker.mu.Lock()
	assert.Equal(t, breakerClosed, breaker.state)
	breaker.mu.Unlock()

	// The threshold-failing request opens the breaker.
	resp, err := transport.RoundTrip(requestWithProxyContext(pc))
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)

	breaker.mu.Lock()
	require.Equal(t, breakerOpen, breaker.state)
	assert.Equal(t, cfg.FailureThreshold, breaker.consecutiveFailures)
	breaker.mu.Unlock()

	// Subsequent requests never reach the upstream and get a 502 with
	// Retry-After.
	before := rt.callCount()
	resp, err = transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })

	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	retryAfter := resp.Header.Get("Retry-After")
	assert.NotEmpty(t, retryAfter)
	seconds, convErr := strconv.Atoi(retryAfter)
	require.NoError(t, convErr)
	assert.Equal(t, 5, seconds)
	assert.Equal(t, before, rt.callCount(), "open breaker must not call the upstream")
}

func TestInstanceBreaker_HalfOpenProbeSuccessClosesBreaker(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	healthy := make(chan struct{})
	rt := &scriptedRoundTripper{
		script: func(_ int) (*http.Response, error) {
			select {
			case <-healthy:
				return jsonResponse(http.StatusOK), nil
			default:
				return nil, errors.New("instance down")
			}
		},
	}
	pc := testProxyContext(nil)
	transport, registry := newBreakerTransport(t, rt, cfg)
	breaker := registry.get(1)

	for range cfg.FailureThreshold {
		_, err := transport.RoundTrip(requestWithProxyContext(pc))
		require.Error(t, err)
	}
	breaker.mu.Lock()
	require.Equal(t, breakerOpen, breaker.state)
	breaker.mu.Unlock()

	// Advance into the half-open window and let the probe succeed.
	clock.advance(cfg.InitialBackoff)
	close(healthy)

	resp, err := transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	breaker.mu.Lock()
	assert.Equal(t, breakerClosed, breaker.state)
	assert.Equal(t, 0, breaker.consecutiveFailures)
	assert.False(t, breaker.lastSuccessAt.IsZero())
	breaker.mu.Unlock()

	// Normal traffic flows again after recovery.
	resp, err = transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestInstanceBreaker_FailedProbeReopensWithDoubledBackoff(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	rt := &scriptedRoundTripper{
		script: func(_ int) (*http.Response, error) {
			return nil, errors.New("still down")
		},
	}
	pc := testProxyContext(nil)
	transport, registry := newBreakerTransport(t, rt, cfg)
	breaker := registry.get(1)

	for range cfg.FailureThreshold {
		_, err := transport.RoundTrip(requestWithProxyContext(pc))
		require.Error(t, err)
	}
	require.Equal(t, breakerOpen, breaker.state)

	clock.advance(cfg.InitialBackoff)

	// First request after the deadline is the probe and fails.
	_, err := transport.RoundTrip(requestWithProxyContext(pc))
	require.Error(t, err)

	breaker.mu.Lock()
	assert.Equal(t, breakerOpen, breaker.state)
	assert.Equal(t, 2*cfg.InitialBackoff, breaker.backoff)
	breaker.mu.Unlock()

	// New requests are rejected with the doubled Retry-After.
	resp, err := transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "10", resp.Header.Get("Retry-After"))
}

func TestInstanceBreaker_HalfOpenAdmitsSingleProbe(t *testing.T) {
	clock := newFakeClock()
	b := newInstanceBreaker(testBreakerConfig(clock))

	// Force the open state, then let the deadline pass.
	b.mu.Lock()
	b.state = breakerOpen
	b.openUntil = clock.now().Add(time.Second)
	b.mu.Unlock()
	clock.advance(time.Second + time.Millisecond)

	ctx := context.Background()
	probe, release, err := b.acquire(ctx)
	require.NoError(t, err)
	assert.True(t, probe)
	t.Cleanup(release)

	_, _, err = b.acquire(ctx)
	require.Error(t, err)
	rejected, ok := errors.AsType[*breakerRejectedError](err)
	require.True(t, ok)
	require.ErrorIs(t, rejected, errBreakerOpen)
	assert.Equal(t, breakerHalfOpenRetryAfter, rejected.retryAfter)
}

func TestInstanceBreaker_ConcurrencyBudgetQueuesAndRejects(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)
	cfg.MaxConcurrent = 2
	cfg.MaxQueued = 1
	b := newInstanceBreaker(cfg)

	// Fill every active slot directly.
	for range cfg.MaxConcurrent {
		b.sem <- struct{}{}
	}

	// One queued request waits for a slot; it occupies the single queue slot.
	queuedDone := make(chan error, 1)
	go func() {
		_, release, err := b.acquire(context.Background())
		if err == nil {
			release()
		}
		queuedDone <- err
	}()
	require.Eventually(t, func() bool {
		return len(b.queue) == 1
	}, time.Second, time.Millisecond)

	// The queue is full: further requests are rejected immediately.
	_, release, err := b.acquire(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errBudgetExhausted)
	if release != nil {
		release()
	}

	// Free one active slot so the queued request proceeds.
	<-b.sem
	select {
	case err := <-queuedDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("queued request did not acquire a slot after one freed")
	}

	// The queue slot was handed back and the budget is usable again.
	assert.Empty(t, b.queue)
	_, rel, err := b.acquire(context.Background())
	require.NoError(t, err)
	rel()
}

func TestInstanceBreaker_QueuedRequestReturnsContextErrorOnCancel(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)
	cfg.MaxConcurrent = 1
	cfg.MaxQueued = 1
	b := newInstanceBreaker(cfg)

	_, activeRelease, err := b.acquire(context.Background())
	require.NoError(t, err)
	t.Cleanup(activeRelease)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, rel, acquireErr := b.acquire(ctx)
		if rel != nil {
			rel()
		}
		done <- acquireErr
	}()
	require.Eventually(t, func() bool {
		return len(b.queue) == 1
	}, time.Second, time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued request did not return on context cancellation")
	}
	assert.Empty(t, b.queue, "cancelled request must release its queue slot")
}

func TestInstanceBreaker_AuthLoginCooldown(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	rt := &scriptedRoundTripper{
		script: func(_ int) (*http.Response, error) {
			return jsonResponse(http.StatusForbidden), nil
		},
	}
	session := &stubSessionRefresher{
		login: func(_ context.Context) error {
			return errors.New("login failed")
		},
	}
	pc := testProxyContext(session)
	transport, registry := newBreakerTransport(t, rt, cfg)
	breaker := registry.get(1)

	// First 403 triggers a login, which fails and is returned as an error.
	resp, err := transport.RoundTrip(requestWithProxyContext(pc))
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	assert.Equal(t, 1, session.loginCalls)
	breaker.mu.Lock()
	assert.Equal(t, 1, breaker.authAttempts)
	assert.Equal(t, 1, breaker.authFailures)
	cooldownUntil := breaker.authCooldownUntil
	breaker.mu.Unlock()

	// While cooling down, no new login is attempted and the request gets a
	// synthetic 403 instead of being replayed.
	resp, err = transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, 1, session.loginCalls, "login must not repeat during cooldown")

	// After the cooldown a login runs again; even though the upstream still
	// answers 403, the request is only replayed once for that refresh.
	clock.advance(cfg.AuthCooldown + time.Second)
	require.True(t, clock.now().After(cooldownUntil))
	session.login = func(_ context.Context) error {
		return nil
	}

	callsBefore := rt.callCount()
	resp, err = transport.RoundTrip(requestWithProxyContext(pc))
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, 2, session.loginCalls)
	assert.Equal(t, callsBefore+2, rt.callCount(), "one original send plus one replay after refresh")
}

func TestInstanceBreaker_AuthLoginIsSingleFlighted(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	rt := &scriptedRoundTripper{
		script: func(call int) (*http.Response, error) {
			if call <= 2 {
				return jsonResponse(http.StatusForbidden), nil
			}
			return jsonResponse(http.StatusOK), nil
		},
	}

	loginStarted := make(chan struct{})
	var startOnce sync.Once
	releaseLogin := make(chan struct{})
	session := &stubSessionRefresher{
		login: func(_ context.Context) error {
			startOnce.Do(func() { close(loginStarted) })
			<-releaseLogin
			return nil
		},
	}
	pc := testProxyContext(session)
	transport, _ := newBreakerTransport(t, rt, cfg)

	type roundTripResult struct {
		resp *http.Response
		err  error
	}
	results := make(chan roundTripResult, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			resp, err := transport.RoundTrip(requestWithProxyContext(pc))
			results <- roundTripResult{resp: resp, err: err}
		}()
	}
	close(start)

	// Wait until both requests have seen their 403 and are parked on the one
	// shared login.
	select {
	case <-loginStarted:
	case <-time.After(time.Second):
		t.Fatal("login never started")
	}
	require.Eventually(t, func() bool {
		return rt.callCount() == 2
	}, time.Second, time.Millisecond)
	assert.Equal(t, 1, session.loginCalls, "only one login may be in flight")

	close(releaseLogin)

	for range 2 {
		select {
		case r := <-results:
			require.NoError(t, r.err)
			require.NotNil(t, r.resp)
			assert.Equal(t, http.StatusOK, r.resp.StatusCode)
			_ = r.resp.Body.Close()
		case <-time.After(time.Second):
			t.Fatal("request waiting on shared login did not finish")
		}
	}
	assert.Equal(t, 1, session.loginCalls, "waiting requests must not trigger more logins")
	assert.Equal(t, 4, rt.callCount(), "each request replayed exactly once after refresh")
}

func TestRetryTransport_NoInstanceContextKeepsPerRequestRetries(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)

	rt := &scriptedRoundTripper{
		script: func(call int) (*http.Response, error) {
			if call == 1 {
				return nil, syscall.ECONNREFUSED
			}
			return jsonResponse(http.StatusOK), nil
		},
	}
	// Registry exists, but the request carries no instance context, so the
	// transport must behave exactly as before.
	transport, registry := newBreakerTransport(t, rt, cfg)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://proxy.example/api/v2/torrents/info", http.NoBody)
	require.NoError(t, err)

	resp, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 2, rt.callCount())
	assert.Empty(t, registry.breakers, "no breaker must be created without instance context")
}

// TestRetryTransport_Breaker502PropagatesThroughReverseProxy drives the full
// httputil.ReverseProxy path against real httptest servers to verify the open
// breaker's 502 and Retry-After reach the HTTP client.
func TestRetryTransport_Breaker502PropagatesThroughReverseProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	require.NoError(t, err)

	clock := newFakeClock()
	cfg := testBreakerConfig(clock)
	registry := newBreakerRegistry(cfg)
	transport := NewRetryTransportWithBreakers(http.DefaultTransport, nil, registry)
	pc := testProxyContext(nil)

	reverse := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstreamURL)
			pr.Out = pr.Out.WithContext(context.WithValue(pr.Out.Context(), proxyContextKey, pc))
		},
		Transport: transport,
	}
	proxyServer := httptest.NewServer(reverse)
	t.Cleanup(proxyServer.Close)

	var last *http.Response
	for range cfg.FailureThreshold + 1 {
		resp, getErr := http.Get(proxyServer.URL)
		require.NoError(t, getErr)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		last = resp
	}

	require.NotNil(t, last)
	assert.Equal(t, http.StatusBadGateway, last.StatusCode)
	assert.NotEmpty(t, last.Header.Get("Retry-After"))
}

func TestRetryTransport_DisabledBreakerKeepsPerRequestRetries(t *testing.T) {
	clock := newFakeClock()
	cfg := testBreakerConfig(clock)
	cfg.Enabled = false

	rt := &scriptedRoundTripper{
		script: func(_ int) (*http.Response, error) {
			return nil, errors.New("down")
		},
	}
	pc := testProxyContext(nil)
	transport, registry := newBreakerTransport(t, rt, cfg)

	for range cfg.FailureThreshold {
		resp, err := transport.RoundTrip(requestWithProxyContext(pc))
		if resp != nil {
			_ = resp.Body.Close()
		}
		require.Error(t, err)
	}
	// Breaker never opened: every request still reached the upstream and no
	// outcomes were recorded.
	breaker := registry.get(pc.instanceID)
	breaker.mu.Lock()
	assert.Equal(t, breakerClosed, breaker.state)
	assert.Equal(t, 0, breaker.totalRequests, "disabled breaker must not record outcomes")
	breaker.mu.Unlock()
}
