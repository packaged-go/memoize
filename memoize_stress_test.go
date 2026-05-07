package memoize

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStressCleanupPreventsEntryRetention(t *testing.T) {
	const entries = 5000
	m := New[string, int, struct{}](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		WithMinimumTTL(time.Millisecond),
		WithMaximumTTL(5*time.Millisecond),
		WithCleanupInterval(0),
	)
	defer m.Close()

	for i := 0; i < entries; i++ {
		if _, err := m.Get("key-"+strconv.Itoa(i), struct{}{}); err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
	}
	if got := m.Len(); got != entries {
		t.Fatalf("expected %d cached entries, got %d", entries, got)
	}

	time.Sleep(10 * time.Millisecond)
	removed := m.Cleanup()
	if removed != entries {
		t.Fatalf("expected cleanup to remove %d entries, got %d", entries, removed)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("expected empty cache after cleanup, got %d entries", got)
	}

	runtime.GC()
	if got := m.Len(); got != 0 {
		t.Fatalf("cache retained entries after GC: %d", got)
	}
}

func TestStressConcurrentAccessUnderRaceDetector(t *testing.T) {
	var calls int64
	m := New[int, int, struct{}](
		func(ctx context.Context, key int, p struct{}) (int, error) {
			atomic.AddInt64(&calls, 1)
			time.Sleep(time.Microsecond)
			return key, nil
		},
		WithMinimumTTL(2*time.Millisecond),
		WithMaximumTTL(20*time.Millisecond),
		WithMaximumResponseTime(time.Millisecond),
		WithCleanupInterval(time.Millisecond),
	)
	defer m.Close()

	const workers = 32
	const iterations = 400
	var wg sync.WaitGroup
	wg.Add(workers)

	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				key := (worker + i) % 64
				if i%11 == 0 {
					m.Delete(key)
				}
				if i%37 == 0 {
					_ = m.Extend(key, 10*time.Millisecond)
				}
				if i%53 == 0 {
					m.Cleanup()
				}

				v, err := m.Get(key, struct{}{})
				if err != nil {
					t.Errorf("get key %d: %v", key, err)
					return
				}
				if v != key {
					t.Errorf("expected value %d, got %d", key, v)
					return
				}
			}
		}(worker)
	}
	wg.Wait()

	if got := atomic.LoadInt64(&calls); got == 0 {
		t.Fatal("expected source to be called")
	}
}

func TestStressSingleflightPerKeyUnderLoad(t *testing.T) {
	const callers = 256
	start := make(chan struct{})
	var calls int64

	m := New[string, int, struct{}](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			atomic.AddInt64(&calls, 1)
			<-start
			return 42, nil
		},
		WithCleanupInterval(0),
	)
	defer m.Close()

	var wg sync.WaitGroup
	wg.Add(callers)
	errs := make(chan error, callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()
			v, err := m.Get("shared", struct{}{})
			if err != nil {
				errs <- err
				return
			}
			if v != 42 {
				errs <- errors.New("unexpected value")
			}
		}()
	}

	deadline := time.Now().Add(time.Second)
	for atomic.LoadInt64(&calls) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt64(&calls); got != 1 {
		t.Fatalf("expected one source call, got %d", got)
	}
}

func TestStressTimedOutCallsAreReleased(t *testing.T) {
	const keys = 128
	release := make(chan struct{})
	var calls int64

	m := New[int, int, struct{}](
		func(ctx context.Context, key int, p struct{}) (int, error) {
			atomic.AddInt64(&calls, 1)
			<-release
			return key, nil
		},
		WithHardTimeout(2*time.Millisecond),
		WithCleanupInterval(0),
	)
	defer func() {
		close(release)
		m.Close()
	}()

	for i := 0; i < keys; i++ {
		_, err := m.Get(i, struct{}{})
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("expected ErrTimeout for key %d, got %v", i, err)
		}
	}

	m.mu.Lock()
	inFlight := len(m.calls)
	m.mu.Unlock()
	if inFlight != keys {
		t.Fatalf("expected %d timed-out call records before cleanup, got %d", keys, inFlight)
	}

	removed := m.Cleanup()
	if removed != keys {
		t.Fatalf("expected cleanup to remove %d timed-out calls, got %d", keys, removed)
	}

	m.mu.Lock()
	inFlight = len(m.calls)
	m.mu.Unlock()
	if inFlight != 0 {
		t.Fatalf("expected no retained timed-out calls, got %d", inFlight)
	}
}

func TestStressBackgroundCleanupKeepsCacheBounded(t *testing.T) {
	const rounds = 8
	const keysPerRound = 250

	m := New[string, int, struct{}](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		WithMinimumTTL(time.Millisecond),
		WithMaximumTTL(5*time.Millisecond),
		WithCleanupInterval(2*time.Millisecond),
	)
	defer m.Close()

	for round := 0; round < rounds; round++ {
		for i := 0; i < keysPerRound; i++ {
			key := strconv.Itoa(round) + "-" + strconv.Itoa(i)
			if _, err := m.Get(key, struct{}{}); err != nil {
				t.Fatalf("round %d key %d: %v", round, i, err)
			}
		}
		time.Sleep(8 * time.Millisecond)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for m.Len() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.Len(); got != 0 {
		t.Fatalf("background cleanup did not release expired entries, retained %d", got)
	}
}

func BenchmarkGetHotCache(b *testing.B) {
	m := New[string, int, struct{}](
		func(ctx context.Context, key string, p struct{}) (int, error) {
			return 1, nil
		},
		WithMinimumTTL(time.Hour),
		WithMaximumTTL(2*time.Hour),
		WithCleanupInterval(0),
	)
	defer m.Close()

	if _, err := m.Get("k", struct{}{}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.Get("k", struct{}{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetHotCacheParallel(b *testing.B) {
	m := New[int, int, struct{}](
		func(ctx context.Context, key int, p struct{}) (int, error) {
			return key, nil
		},
		WithMinimumTTL(time.Hour),
		WithMaximumTTL(2*time.Hour),
		WithCleanupInterval(0),
	)
	defer m.Close()

	for i := 0; i < 128; i++ {
		if _, err := m.Get(i, struct{}{}); err != nil {
			b.Fatal(err)
		}
	}

	var next uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := int(atomic.AddUint64(&next, 1) % 128)
			if _, err := m.Get(key, struct{}{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGetColdDistinctKeys(b *testing.B) {
	m := New[int, int, struct{}](
		func(ctx context.Context, key int, p struct{}) (int, error) {
			return key, nil
		},
		WithCleanupInterval(0),
	)
	defer m.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.Get(i, struct{}{}); err != nil {
			b.Fatal(err)
		}
	}
}
