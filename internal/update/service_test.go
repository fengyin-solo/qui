// Copyright (c) 2025-2026, s0up and the autobrr contributors.
// SPDX-License-Identifier: GPL-2.0-or-later

package update

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autobrr/qui/pkg/version"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type fakeReleaseChecker struct {
	mu        sync.Mutex
	calls     int
	available bool
	release   *version.Release
	err       error

	entered     chan struct{}
	enteredOnce sync.Once
	gate        chan struct{}
}

func (c *fakeReleaseChecker) CheckNewVersion(ctx context.Context, _ string) (bool, *version.Release, error) {
	c.mu.Lock()
	c.calls++
	if c.entered != nil {
		entered := c.entered
		c.enteredOnce.Do(func() { close(entered) })
	}
	c.mu.Unlock()

	if c.gate != nil {
		select {
		case <-c.gate:
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
	}
	return c.available, c.release, c.err
}

func (c *fakeReleaseChecker) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newTestService(t *testing.T, enabled bool, checker *fakeReleaseChecker) *Service {
	t.Helper()
	svc := NewService(zerolog.New(io.Discard), enabled, testCurrentVersion, "test-agent")
	svc.releaseChecker = checker
	return svc
}

func releaseFor(tag string) *version.Release {
	return &version.Release{TagName: tag}
}

func TestServiceCheckUpdatesDisabledDoesNothing(t *testing.T) {
	checker := &fakeReleaseChecker{}
	svc := newTestService(t, false, checker)

	svc.CheckUpdates(context.Background())

	require.Zero(t, checker.callCount())
	require.Nil(t, svc.GetLatestRelease(context.Background()))
}

func TestServiceCachesAvailableRelease(t *testing.T) {
	checker := &fakeReleaseChecker{available: true, release: releaseFor("v1.2.3")}
	svc := newTestService(t, true, checker)

	release, err := svc.CheckUpdateAvailable(context.Background())
	require.NoError(t, err)
	require.Equal(t, "v1.2.3", release.TagName)

	cached := svc.GetLatestRelease(context.Background())
	require.NotNil(t, cached)
	require.Equal(t, "v1.2.3", cached.TagName)
}

func TestServiceConcurrentChecksShareOneNetworkCall(t *testing.T) {
	checker := &fakeReleaseChecker{
		available: true,
		release:   releaseFor("v1.2.3"),
		entered:   make(chan struct{}),
		gate:      make(chan struct{}),
	}
	svc := newTestService(t, true, checker)

	const callers = 20
	var wg sync.WaitGroup
	results := make([]*version.Release, callers)
	errs := make([]error, callers)
	start := make(chan struct{})

	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.CheckUpdateAvailable(context.Background())
		}(i)
	}

	close(start)
	<-checker.entered

	// Wait until every other caller has joined the leader's flight before
	// releasing the gate; otherwise a straggler could legitimately start a
	// second flight after the first completes.
	require.Eventually(t, func() bool {
		svc.flightMu.Lock()
		defer svc.flightMu.Unlock()
		return svc.flight != nil && svc.flight.waiters == callers-1
	}, time.Second, time.Millisecond)
	require.Equal(t, 1, checker.callCount())
	close(checker.gate)
	wg.Wait()

	require.Equal(t, 1, checker.callCount())
	for i := range callers {
		require.NoError(t, errs[i])
		require.Equal(t, "v1.2.3", results[i].TagName)
	}
}

func TestServiceSetEnabledClearsCachedRelease(t *testing.T) {
	checker := &fakeReleaseChecker{available: true, release: releaseFor("v1.2.3")}
	svc := newTestService(t, true, checker)

	_, err := svc.CheckUpdateAvailable(context.Background())
	require.NoError(t, err)
	require.NotNil(t, svc.GetLatestRelease(context.Background()))

	svc.SetEnabled(false)
	require.Nil(t, svc.GetLatestRelease(context.Background()))

	svc.CheckUpdates(context.Background())
	require.Equal(t, 1, checker.callCount(), "disabled CheckUpdates must not hit the checker")

	svc.SetEnabled(true)
	svc.CheckUpdates(context.Background())
	require.Equal(t, 2, checker.callCount())
}

func TestServiceCheckErrorIsNotCached(t *testing.T) {
	checker := &fakeReleaseChecker{err: errors.New("network down")}
	svc := newTestService(t, true, checker)

	_, err := svc.CheckUpdateAvailable(context.Background())
	require.Error(t, err)
	require.Nil(t, svc.GetLatestRelease(context.Background()))
}

func TestServiceUnavailableReleaseClearsCache(t *testing.T) {
	checker := &fakeReleaseChecker{available: true, release: releaseFor("v1.2.3")}
	svc := newTestService(t, true, checker)

	_, err := svc.CheckUpdateAvailable(context.Background())
	require.NoError(t, err)
	require.NotNil(t, svc.GetLatestRelease(context.Background()))

	checker.available = false
	checker.release = nil
	_, err = svc.CheckUpdateAvailable(context.Background())
	require.NoError(t, err)
	require.Nil(t, svc.GetLatestRelease(context.Background()))
}

// Ensure the fake exercises the atomic counter path under -race even when
// individual tests don't.
func TestFakeReleaseCheckerCountsCalls(t *testing.T) {
	checker := &fakeReleaseChecker{}
	var n atomic.Int64
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			_, _, _ = checker.CheckNewVersion(context.Background(), "1.0.0")
			n.Add(1)
		})
	}
	wg.Wait()
	require.Equal(t, int(n.Load()), checker.callCount())
}
