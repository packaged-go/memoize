// Package memoize provides typed memoization for slow external lookups.
//
// A Memoizer stores successful responses until their maximum TTL, avoids
// refreshes during the minimum TTL, and coordinates refreshes so only one source
// call per key is active at a time. Once a cached response is outside the
// minimum TTL, Get attempts a refresh but can return the stale response when
// the configured maximum response time is exceeded.
//
// Source functions receive context, key, and a caller-defined params value:
//
//	type LookupParams struct {
//		Region string
//	}
//
//	cache := memoize.New[string, LookupParams, User](loadUser,
//		memoize.WithMinimumTTL(30*time.Second),
//		memoize.WithMaximumTTL(5*time.Minute),
//		memoize.WithMaximumResponseTime(50*time.Millisecond),
//		memoize.WithHardTimeout(2*time.Second),
//	)
//
//	user, err := cache.Get("user:123", LookupParams{Region: "eu"})
//
// Errors can opt into a specific cache duration by returning
// memoize.WithErrorTTL(err, ttl). Unwrapped errors use the configured maximum
// error TTL.
//
// Expired entries are removed by a background cleanup process. Call Close when
// a Memoizer is no longer needed to stop that process.
package memoize
