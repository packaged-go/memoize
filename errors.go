package memoize

import (
	"errors"
	"time"
)

var (
	// ErrTimeout is returned when a fetch cannot complete before the configured hard timeout.
	ErrTimeout = errors.New("memoize: fetch timed out")

	// ErrNotFound is returned by cache control methods when a key is not cached.
	ErrNotFound = errors.New("memoize: key not found")

	// ErrClosed is returned when a memoizer has been closed.
	ErrClosed = errors.New("memoize: closed")
)

type ttlError struct {
	err error
	ttl time.Duration
}

func (e ttlError) Error() string {
	return e.err.Error()
}

func (e ttlError) Unwrap() error {
	return e.err
}

// WithErrorTTL wraps an error with the amount of time that error should be cached.
//
// A non-positive ttl disables caching for this specific error.
func WithErrorTTL(err error, ttl time.Duration) error {
	if err == nil {
		return nil
	}
	return ttlError{err: err, ttl: ttl}
}

func errorTTL(err error) (time.Duration, bool) {
	var ttlErr ttlError
	if errors.As(err, &ttlErr) {
		return ttlErr.ttl, true
	}
	return 0, false
}
