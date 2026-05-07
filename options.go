package memoize

import "time"

type options struct {
	minTTL          time.Duration // minTTL is the period after a successful fetch where cached values are returned
	maxTTL          time.Duration // maxTTL is the hard lifetime for successful cached values
	maxResponseTime time.Duration // maxResponseTime is how long Get waits for a refresh before returning a stale cached value
	hardTimeout     time.Duration // hardTimeout is the maximum time a source call may run before its context is cancelled
	maxErrorTTL     time.Duration // maxErrorTTL is the default and upper bound TTL for cached errors
	cleanupInterval time.Duration // cleanupInterval is how frequently expired entries are removed from memory. A non-positive interval disables background cleanup.
}

func defaultOptions() options {
	return options{
		minTTL:          time.Second * 5,
		maxTTL:          time.Hour,
		maxResponseTime: time.Millisecond * 300,
		hardTimeout:     time.Second * 10,
		maxErrorTTL:     time.Minute,
		cleanupInterval: time.Minute * 10,
	}
}

// Option configures a Memoizer.
type Option func(*options)

// WithMinimumTTL sets the period after a successful fetch where cached values
// are returned without attempting another source call.
func WithMinimumTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.minTTL = ttl
	}
}

// WithMaximumTTL sets the hard lifetime for successful cached values.
func WithMaximumTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.maxTTL = ttl
	}
}

// WithMaximumResponseTime sets how long Get waits for a refresh before
// returning a stale cached value. It does not cancel the source call.
func WithMaximumResponseTime(timeout time.Duration) Option {
	return func(o *options) {
		o.maxResponseTime = timeout
	}
}

// WithHardTimeout sets the maximum time a source call may run before its
// context is cancelled. Cold misses return ErrTimeout when this deadline is hit.
func WithHardTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.hardTimeout = timeout
	}
}

// WithMaximumErrorTTL sets the default and upper bound TTL for cached errors.
func WithMaximumErrorTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.maxErrorTTL = ttl
	}
}

// WithCleanupInterval sets how frequently expired entries are removed from
// memory. A non-positive interval disables background cleanup.
func WithCleanupInterval(interval time.Duration) Option {
	return func(o *options) {
		o.cleanupInterval = interval
	}
}
