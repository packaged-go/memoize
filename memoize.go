package memoize

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SourceFunc fetches a value for key using caller-supplied params.
type SourceFunc[K comparable, V any, P any] func(context.Context, K, P) (V, error)

// KeySourceFunc fetches a value for key without caller-supplied params.
type KeySourceFunc[K comparable, V any] func(context.Context, K) (V, error)

// Memoizer caches source responses and coordinates refreshes so only one source
// call for a key is active at a time.
type Memoizer[K comparable, V any, P any] struct {
	source SourceFunc[K, V, P]
	opts   options

	mu       sync.Mutex
	entries  map[K]entry[V]
	calls    map[K]*call[V]
	versions map[K]uint64
	clearGen uint64

	closeOnce sync.Once
	closed    chan struct{}
	isClosed  bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

type entry[V any] struct {
	value     V
	err       error
	fetchedAt time.Time
	expiresAt time.Time
}

type call[V any] struct {
	done      chan struct{}
	startedAt time.Time
	cancel    context.CancelFunc
	version   uint64
	clearGen  uint64
	value     V
	err       error
}

// New creates a Memoizer for source.
func New[K comparable, V any, P any](source SourceFunc[K, V, P], opts ...Option) *Memoizer[K, V, P] {
	cfg := defaultOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxTTL < cfg.minTTL {
		cfg.maxTTL = cfg.minTTL
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Memoizer[K, V, P]{
		source:   source,
		opts:     cfg,
		entries:  make(map[K]entry[V]),
		calls:    make(map[K]*call[V]),
		versions: make(map[K]uint64),
		closed:   make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}
	if cfg.cleanupInterval > 0 {
		go m.cleanupLoop()
	}
	return m
}

// NewKeyed creates a Memoizer for a source that only needs context and key.
func NewKeyed[K comparable, V any](source KeySourceFunc[K, V], opts ...Option) *Memoizer[K, V, Params] {
	return New[K, V, Params](
		func(ctx context.Context, key K, _ Params) (V, error) {
			return source(ctx, key)
		},
		opts...,
	)
}

// Get returns a cached value when it is within the minimum TTL. Past that point
// it starts or joins a single refresh call for the key. If that refresh does not
// finish within the configured maximum response time, Get returns the stale
// cached response as long as it has not passed the maximum TTL.
//
// If params is omitted, the source receives the zero value for P.
func (m *Memoizer[K, V, P]) Get(key K, params ...P) (V, error) {
	return m.get(key, optionalParam(params), false)
}

// GetFresh starts or joins a refresh once the cached response is outside the
// minimum TTL, and waits for that refresh unless the context or hard timeout
// expires. If a stale response is still inside the maximum TTL, it is returned
// when the wait deadline is hit.
//
// If params is omitted, the source receives the zero value for P.
func (m *Memoizer[K, V, P]) GetFresh(key K, params ...P) (V, error) {
	return m.get(key, optionalParam(params), true)
}

// Close stops the background cleanup process, cancels in-flight source calls,
// and waits for those calls to return. Future Get calls return ErrClosed.
func (m *Memoizer[K, V, P]) Close() {
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.isClosed = true
		inFlight := len(m.calls)
		for _, c := range m.calls {
			c.cancel()
		}
		m.calls = make(map[K]*call[V])
		m.mu.Unlock()

		m.cancel()
		close(m.closed)
		m.debug("memoize.close", zap.Int("in_flight", inFlight))
	})

	m.wg.Wait()
	m.debug("memoize.close.complete")
}

// Delete removes key from the cache.
func (m *Memoizer[K, V, P]) Delete(key K) {
	m.mu.Lock()
	_, hadEntry := m.entries[key]
	if _, hasCall := m.calls[key]; hasCall {
		m.versions[key]++
	} else {
		delete(m.versions, key)
	}
	_, hasCall := m.calls[key]
	delete(m.entries, key)
	m.mu.Unlock()
	m.debugForKey("memoize.delete", key, zap.Bool("had_entry", hadEntry), zap.Bool("has_in_flight_call", hasCall))
}

// Clear removes all cached entries. In-flight calls are not cancelled, but
// their eventual results will not repopulate the cache.
func (m *Memoizer[K, V, P]) Clear() {
	m.mu.Lock()
	entries := len(m.entries)
	calls := len(m.calls)
	m.clearGen++
	m.entries = make(map[K]entry[V])
	m.versions = make(map[K]uint64)
	m.mu.Unlock()
	m.debug("memoize.clear", zap.Int("entries", entries), zap.Int("in_flight_calls", calls))
}

// Len returns the number of cached entries currently held in memory.
func (m *Memoizer[K, V, P]) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.entries)
}

// Extend extends the maximum usable lifetime for a cached key.
func (m *Memoizer[K, V, P]) Extend(key K, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	ent, ok := m.entries[key]
	if !ok {
		m.debugForKey("memoize.extend.miss", key, zap.Duration("ttl", ttl))
		return ErrNotFound
	}
	ent.expiresAt = time.Now().Add(ttl)
	m.entries[key] = ent
	m.debugForKey("memoize.extend", key, zap.Duration("ttl", ttl), zap.Time("expires_at", ent.expiresAt))
	return nil
}

// Cleanup removes expired cached entries and timed-out in-flight call records,
// and returns the number removed.
func (m *Memoizer[K, V, P]) Cleanup() int {
	now := time.Now()
	removed := 0

	m.mu.Lock()
	for key, ent := range m.entries {
		if !now.Before(ent.expiresAt) {
			delete(m.entries, key)
			removed++
		}
	}
	removed += m.cleanupExpiredCallsLocked(now)
	m.mu.Unlock()

	m.debug("memoize.cleanup", zap.Int("removed", removed))
	return removed
}

func optionalParam[P any](params []P) P {
	var p P
	if len(params) > 0 {
		p = params[0]
	}
	return p
}

func (m *Memoizer[K, V, P]) debug(message string, fields ...zap.Field) {
	if m.opts.debugLogger == nil {
		return
	}
	m.opts.debugLogger.Debug(message, fields...)
}

func (m *Memoizer[K, V, P]) debugForKey(message string, key K, fields ...zap.Field) {
	if m.opts.debugLogger == nil {
		return
	}
	base := []zap.Field{
		zap.String("route", fmt.Sprint(key)),
		zap.Any("key", key),
	}
	m.opts.debugLogger.Debug(message, append(base, fields...)...)
}

func (m *Memoizer[K, V, P]) get(key K, params P, waitForFresh bool) (V, error) {
	now := time.Now()
	operation := "get"
	if waitForFresh {
		operation = "get_fresh"
	}
	m.debugForKey("memoize.request.start", key,
		zap.String("operation", operation),
		zap.Any("params", params),
		zap.Duration("min_ttl", m.opts.minTTL),
		zap.Duration("max_ttl", m.opts.maxTTL),
		zap.Duration("max_response_time", m.opts.maxResponseTime),
		zap.Duration("hard_timeout", m.opts.hardTimeout),
	)

	m.mu.Lock()
	if m.isClosed {
		m.mu.Unlock()
		var zero V
		m.debugForKey("memoize.request.closed", key, zap.String("operation", operation))
		return zero, ErrClosed
	}

	ent, hasEntry := m.entries[key]
	if hasEntry && ent.err == nil && now.Before(ent.fetchedAt.Add(m.opts.minTTL)) && now.Before(ent.expiresAt) {
		m.mu.Unlock()
		m.debugForKey("memoize.cache.hit", key,
			zap.String("operation", operation),
			zap.String("reason", "within_min_ttl"),
			zap.Time("fetched_at", ent.fetchedAt),
			zap.Time("expires_at", ent.expiresAt),
		)
		return ent.value, nil
	}
	if hasEntry && ent.err != nil && now.Before(ent.expiresAt) {
		m.mu.Unlock()
		m.debugForKey("memoize.cache.hit", key,
			zap.String("operation", operation),
			zap.String("reason", "cached_error"),
			zap.Error(ent.err),
			zap.Time("fetched_at", ent.fetchedAt),
			zap.Time("expires_at", ent.expiresAt),
		)
		return ent.value, ent.err
	}

	staleOK := hasEntry && ent.err == nil && now.Before(ent.expiresAt)
	c := m.getOrStartCallLocked(key, params)
	m.mu.Unlock()

	if !hasEntry {
		m.debugForKey("memoize.cache.miss", key, zap.String("operation", operation))
		return m.waitForColdMiss(c)
	}

	if waitForFresh {
		m.debugForKey("memoize.refresh.wait", key,
			zap.String("operation", operation),
			zap.Bool("stale_available", staleOK),
		)
		return m.waitForFresh(c, ent, staleOK)
	}

	if m.opts.maxResponseTime <= 0 {
		if staleOK {
			m.debugForKey("memoize.stale.return", key,
				zap.String("operation", operation),
				zap.String("reason", "max_response_time_disabled"),
				zap.Time("fetched_at", ent.fetchedAt),
				zap.Time("expires_at", ent.expiresAt),
			)
			return ent.value, nil
		}
		m.debugForKey("memoize.refresh.wait", key,
			zap.String("operation", operation),
			zap.Bool("stale_available", false),
		)
		return m.waitForColdMiss(c)
	}

	timer := time.NewTimer(m.opts.maxResponseTime)
	defer timer.Stop()

	select {
	case <-c.done:
		m.debugForKey("memoize.refresh.return", key,
			zap.String("operation", operation),
			zap.Duration("waited", time.Since(now)),
			zap.Error(c.err),
		)
		return c.value, c.err
	case <-timer.C:
		if staleOK {
			m.debugForKey("memoize.stale.return", key,
				zap.String("operation", operation),
				zap.String("reason", "max_response_time_exceeded"),
				zap.Duration("max_response_time", m.opts.maxResponseTime),
				zap.Time("fetched_at", ent.fetchedAt),
				zap.Time("expires_at", ent.expiresAt),
			)
			return ent.value, nil
		}
		m.debugForKey("memoize.refresh.wait", key,
			zap.String("operation", operation),
			zap.Bool("stale_available", false),
			zap.String("reason", "max_response_time_exceeded_without_stale"),
		)
		return m.waitForColdMiss(c)
	}
}

func (m *Memoizer[K, V, P]) cleanupLoop() {
	ticker := time.NewTicker(m.opts.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.Cleanup()
		case <-m.closed:
			return
		}
	}
}

func (m *Memoizer[K, V, P]) cleanupExpiredCallsLocked(now time.Time) int {
	if m.opts.hardTimeout <= 0 {
		return 0
	}

	removed := 0
	for key, c := range m.calls {
		if m.callExpiredAt(c, now) {
			c.cancel()
			delete(m.calls, key)
			m.cleanupVersionLocked(key)
			removed++
			m.debugForKey("memoize.call.cleanup_expired", key,
				zap.Time("started_at", c.startedAt),
				zap.Duration("hard_timeout", m.opts.hardTimeout),
			)
		}
	}
	return removed
}

func (m *Memoizer[K, V, P]) callExpired(c *call[V], now time.Time) bool {
	return m.opts.hardTimeout > 0 && m.callExpiredAt(c, now)
}

func (m *Memoizer[K, V, P]) callExpiredAt(c *call[V], now time.Time) bool {
	return !now.Before(c.startedAt.Add(m.opts.hardTimeout))
}

func (m *Memoizer[K, V, P]) getOrStartCallLocked(key K, params P) *call[V] {
	if c, ok := m.calls[key]; ok {
		if !m.callExpired(c, time.Now()) {
			m.debugForKey("memoize.call.join", key,
				zap.Time("started_at", c.startedAt),
				zap.Uint64("version", c.version),
				zap.Uint64("clear_generation", c.clearGen),
			)
			return c
		}
		c.cancel()
		delete(m.calls, key)
		m.debugForKey("memoize.call.replace_expired", key,
			zap.Time("started_at", c.startedAt),
			zap.Duration("hard_timeout", m.opts.hardTimeout),
		)
	}

	ctx := m.ctx
	var cancel context.CancelFunc
	if m.opts.hardTimeout > 0 {
		ctx, cancel = context.WithTimeout(m.ctx, m.opts.hardTimeout)
	} else {
		ctx, cancel = context.WithCancel(m.ctx)
	}

	c := &call[V]{
		done:      make(chan struct{}),
		startedAt: time.Now(),
		cancel:    cancel,
		version:   m.versions[key],
		clearGen:  m.clearGen,
	}
	m.calls[key] = c
	m.wg.Add(1)
	m.debugForKey("memoize.call.start", key,
		zap.Time("started_at", c.startedAt),
		zap.Uint64("version", c.version),
		zap.Uint64("clear_generation", c.clearGen),
		zap.Duration("hard_timeout", m.opts.hardTimeout),
	)
	go m.fetch(ctx, key, params, c)
	return c
}

func (m *Memoizer[K, V, P]) fetch(ctx context.Context, key K, params P, c *call[V]) {
	defer m.wg.Done()
	defer c.cancel()

	m.debugForKey("memoize.source.start", key,
		zap.Time("started_at", c.startedAt),
		zap.Any("params", params),
	)
	value, err := m.source(ctx, key, params)
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) && m.opts.hardTimeout > 0 {
		err = ErrTimeout
	}

	now := time.Now()
	ent, cacheable := m.entryForResult(value, err, now)
	m.debugForKey("memoize.source.finish", key,
		zap.Duration("duration", now.Sub(c.startedAt)),
		zap.Bool("cacheable", cacheable),
		zap.Error(err),
	)

	m.mu.Lock()
	cached := false
	closed := m.isClosed
	currentVersion := m.versions[key]
	currentClearGen := m.clearGen
	if cacheable && !m.isClosed && c.version == m.versions[key] && c.clearGen == m.clearGen {
		currentEntry, hasCurrentEntry := m.entries[key]
		if !hasCurrentEntry || !currentEntry.fetchedAt.After(c.startedAt) {
			m.entries[key] = ent
			cached = true
		}
	}
	c.value = value
	c.err = err
	if current, ok := m.calls[key]; ok && current == c {
		delete(m.calls, key)
	}
	m.cleanupVersionLocked(key)
	close(c.done)
	m.mu.Unlock()
	m.debugForKey("memoize.source.result", key,
		zap.Bool("cached", cached),
		zap.Bool("cacheable", cacheable),
		zap.Bool("closed", closed),
		zap.Uint64("call_version", c.version),
		zap.Uint64("current_version", currentVersion),
		zap.Uint64("call_clear_generation", c.clearGen),
		zap.Uint64("current_clear_generation", currentClearGen),
		zap.Error(err),
	)
}

func (m *Memoizer[K, V, P]) cleanupVersionLocked(key K) {
	if _, hasEntry := m.entries[key]; hasEntry {
		return
	}
	if _, hasCall := m.calls[key]; hasCall {
		return
	}
	delete(m.versions, key)
}

func (m *Memoizer[K, V, P]) entryForResult(value V, err error, now time.Time) (entry[V], bool) {
	if err == nil {
		return entry[V]{
			value:     value,
			fetchedAt: now,
			expiresAt: now.Add(m.opts.maxTTL),
		}, true
	}
	if errors.Is(err, ErrTimeout) || errors.Is(err, context.Canceled) {
		return entry[V]{}, false
	}

	ttl, ok := errorTTL(err)
	if !ok {
		ttl = m.opts.maxErrorTTL
	}
	if m.opts.maxErrorTTL > 0 && ttl > m.opts.maxErrorTTL {
		ttl = m.opts.maxErrorTTL
	}
	if ttl <= 0 {
		return entry[V]{}, false
	}

	return entry[V]{
		value:     value,
		err:       err,
		fetchedAt: now,
		expiresAt: now.Add(ttl),
	}, true
}

func (m *Memoizer[K, V, P]) waitForColdMiss(c *call[V]) (V, error) {
	if m.opts.hardTimeout > 0 {
		return waitWithHardTimeout(c, m.opts.hardTimeout, entry[V]{}, false)
	}

	<-c.done
	return c.value, c.err
}

func (m *Memoizer[K, V, P]) waitForFresh(c *call[V], stale entry[V], staleOK bool) (V, error) {
	if m.opts.hardTimeout > 0 {
		return waitWithHardTimeout(c, m.opts.hardTimeout, stale, staleOK)
	}

	<-c.done
	return c.value, c.err
}

func waitWithHardTimeout[V any](c *call[V], timeout time.Duration, stale entry[V], staleOK bool) (V, error) {
	remaining := time.Until(c.startedAt.Add(timeout))
	if remaining <= 0 {
		c.cancel()
		if staleOK {
			return stale.value, nil
		}
		var zero V
		return zero, ErrTimeout
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-c.done:
		return c.value, c.err
	case <-timer.C:
		c.cancel()
		if staleOK {
			return stale.value, nil
		}
		var zero V
		return zero, ErrTimeout
	}
}
