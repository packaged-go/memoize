package memoize_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/packaged-go/memoize"
)

type params struct {
	Offset int
}

func TestGetCachesWithinMinimumTTL(t *testing.T) {
	var calls int32
	m := memoize.New[string, params, int](
		func(ctx context.Context, key string, p params) (int, error) {
			return int(atomic.AddInt32(&calls, 1)) + p.Offset, nil
		},
		memoize.WithMinimumTTL(time.Hour),
		memoize.WithMaximumTTL(2*time.Hour),
	)
	defer m.Close()

	v, err := m.Get("k", params{Offset: 10})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 11 {
		t.Fatalf("expected first value 11, got %d", v)
	}

	v, err = m.Get("k", params{Offset: 20})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 11 {
		t.Fatalf("expected cached value 11, got %d", v)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected one source call, got %d", got)
	}
}

func TestGetReturnsStaleWhenRefreshExceedsResponseTime(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32

	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			call := atomic.AddInt32(&calls, 1)
			if call == 1 {
				return 1, nil
			}
			close(started)
			<-release
			return 2, nil
		},
		memoize.WithMinimumTTL(10*time.Millisecond),
		memoize.WithMaximumTTL(time.Hour),
		memoize.WithMaximumResponseTime(20*time.Millisecond),
	)
	defer m.Close()

	if _, err := m.Get("k", struct{}{}); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	time.Sleep(15 * time.Millisecond)

	v, err := m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected stale value 1, got %d", v)
	}

	<-started
	close(release)
	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&calls) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	v, err = m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("unexpected error after refresh: %v", err)
	}
	if v != 2 {
		t.Fatalf("expected refreshed value 2, got %d", v)
	}
}

func TestColdMissWaitsForSource(t *testing.T) {
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			time.Sleep(25 * time.Millisecond)
			return 7, nil
		},
		memoize.WithMaximumResponseTime(time.Millisecond),
	)
	defer m.Close()

	v, err := m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != 7 {
		t.Fatalf("expected cold miss value 7, got %d", v)
	}
}

func TestColdMissHardTimeout(t *testing.T) {
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		},
		memoize.WithHardTimeout(10*time.Millisecond),
	)
	defer m.Close()

	_, err := m.Get("k", struct{}{})
	if !errors.Is(err, memoize.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
}

func TestColdMissHardTimeoutWhenSourceIgnoresContext(t *testing.T) {
	release := make(chan struct{})
	var calls int32
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			atomic.AddInt32(&calls, 1)
			<-release
			return 9, nil
		},
		memoize.WithHardTimeout(10*time.Millisecond),
	)
	defer m.Close()

	start := time.Now()
	_, err := m.Get("k", struct{}{})
	if !errors.Is(err, memoize.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("hard timeout took too long: %s", elapsed)
	}

	close(release)
	time.Sleep(5 * time.Millisecond)
	v, err := m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("expected eventual cached result, got %v", err)
	}
	if v != 9 {
		t.Fatalf("expected eventual cached result 9, got %d", v)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected late timed-out result to be discarded and refetched, got %d calls", got)
	}
}

func TestTimedOutCallDoesNotBlockFutureFetches(t *testing.T) {
	releaseFirst := make(chan struct{})
	var calls int32
	firstCtxDone := make(chan struct{})
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			call := atomic.AddInt32(&calls, 1)
			if call == 1 {
				<-ctx.Done()
				close(firstCtxDone)
				<-releaseFirst
				return 1, nil
			}
			return 2, nil
		},
		memoize.WithHardTimeout(10*time.Millisecond),
		memoize.WithCleanupInterval(5*time.Millisecond),
	)
	defer m.Close()

	_, err := m.Get("k", struct{}{})
	if !errors.Is(err, memoize.ErrTimeout) {
		t.Fatalf("expected ErrTimeout, got %v", err)
	}
	select {
	case <-firstCtxDone:
	case <-time.After(time.Second):
		t.Fatal("expected timed-out call context to be cancelled")
	}

	v, err := m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("expected second fetch to succeed, got %v", err)
	}
	if v != 2 {
		t.Fatalf("expected second fetch value 2, got %d", v)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected two source calls, got %d", got)
	}

	close(releaseFirst)
	time.Sleep(5 * time.Millisecond)
	v, err = m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("expected cached second value, got %v", err)
	}
	if v != 2 {
		t.Fatalf("old timed-out call overwrote newer value: got %d", v)
	}
}

func TestErrorTTL(t *testing.T) {
	errSource := errors.New("source failed")
	var calls int32
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			atomic.AddInt32(&calls, 1)
			return 0, memoize.WithErrorTTL(errSource, 20*time.Millisecond)
		},
		memoize.WithMaximumErrorTTL(time.Minute),
	)
	defer m.Close()

	_, err := m.Get("k", struct{}{})
	if !errors.Is(err, errSource) {
		t.Fatalf("expected wrapped source error, got %v", err)
	}
	_, err = m.Get("k", struct{}{})
	if !errors.Is(err, errSource) {
		t.Fatalf("expected cached source error, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected cached error to avoid a second call, got %d", got)
	}

	time.Sleep(25 * time.Millisecond)
	_, _ = m.Get("k", struct{}{})
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected error cache expiry to fetch again, got %d calls", got)
	}
}

func TestSingleflightPerKey(t *testing.T) {
	var calls int32
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&calls, 1)
			return 3, nil
		},
	)
	defer m.Close()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := m.Get("k", struct{}{})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if v != 3 {
				t.Errorf("expected 3, got %d", v)
			}
		}()
	}
	wg.Wait()

	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected one source call, got %d", got)
	}
}

func TestCleanupRemovesExpiredEntries(t *testing.T) {
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		memoize.WithMinimumTTL(time.Millisecond),
		memoize.WithMaximumTTL(10*time.Millisecond),
		memoize.WithCleanupInterval(0),
	)
	defer m.Close()

	if _, err := m.Get("k", struct{}{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("expected one cached entry, got %d", got)
	}

	time.Sleep(15 * time.Millisecond)
	if removed := m.Cleanup(); removed != 1 {
		t.Fatalf("expected one removed entry, got %d", removed)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected cache to be empty, got %d entries", got)
	}
}

func TestBackgroundCleanupRemovesExpiredEntries(t *testing.T) {
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		memoize.WithMinimumTTL(time.Millisecond),
		memoize.WithMaximumTTL(10*time.Millisecond),
		memoize.WithCleanupInterval(5*time.Millisecond),
	)
	defer m.Close()

	if _, err := m.Get("k", struct{}{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for m.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected background cleanup to empty cache, got %d entries", got)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		memoize.WithCleanupInterval(time.Millisecond),
	)

	m.Close()
	m.Close()
}

func TestCloseCancelsInFlightAndRejectsNewGets(t *testing.T) {
	started := make(chan struct{})
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		},
	)

	done := make(chan error, 1)
	go func() {
		_, err := m.Get("k", struct{}{})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("source did not start")
	}

	m.Close()

	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled in-flight get, got %v", err)
	}

	_, err = m.Get("k", struct{}{})
	if !errors.Is(err, memoize.ErrClosed) {
		t.Fatalf("expected ErrClosed after Close, got %v", err)
	}
}

func TestDeletePreventsInFlightResultFromRepopulatingCache(t *testing.T) {
	release := make(chan struct{})
	var calls int32
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			call := atomic.AddInt32(&calls, 1)
			if call == 1 {
				<-release
			}
			return int(call), nil
		},
	)
	defer m.Close()

	done := make(chan error, 1)
	go func() {
		_, err := m.Get("k", struct{}{})
		done <- err
	}()

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt32(&calls) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatal("source did not start")
	}

	m.Delete("k")
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("unexpected in-flight error: %v", err)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected delete to prevent late cache write, got %d entries", got)
	}

	v, err := m.Get("k", struct{}{})
	if err != nil {
		t.Fatalf("unexpected second get error: %v", err)
	}
	if v != 2 {
		t.Fatalf("expected fresh value 2 after delete, got %d", v)
	}
}

func TestClearPreventsInFlightResultFromRepopulatingCache(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	m := memoize.New[string, struct{}, int](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			close(started)
			<-release
			return 1, nil
		},
	)
	defer m.Close()

	done := make(chan error, 1)
	go func() {
		_, err := m.Get("k", struct{}{})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("source did not start")
	}

	m.Clear()
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("unexpected in-flight error: %v", err)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected clear to prevent late cache write, got %d entries", got)
	}
}
