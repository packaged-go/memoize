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
//	cache := memoize.New[string, User, LookupParams](loadUser,
//		memoize.WithMinimumTTL(30*time.Second),
//		memoize.WithMaximumTTL(5*time.Minute),
//		memoize.WithMaximumResponseTime(50*time.Millisecond),
//		memoize.WithHardTimeout(2*time.Second),
//	)
//
//	user, err := cache.Get("user:123", LookupParams{Region: "eu"})
//
// Params are optional at call time. When omitted, the source receives the zero
// value for the params type. For key-only sources, use NewKeyed:
//
//	cache := memoize.NewKeyed[string, User](loadUser)
//	user, err := cache.Get("user:123")
//
// If the params type is nilable, such as *LookupParams or any, an omitted
// params argument is passed to the source as nil.
//
// The package Params type can be used when a source wants optional dynamic
// params without defining a custom params struct:
//
//	cache := memoize.New[string, User, memoize.Params](loadUser)
//	user, err := cache.Get("user:123")
//	user, err = cache.Get("user:123", memoize.NoParams())
//
// WithDebugging attaches a zap logger for debug-level trace events covering
// cache hits, misses, refreshes, stale returns, source calls, invalidation, and
// cleanup:
//
//	cache := memoize.NewKeyed[string, User](loadUser,
//		memoize.WithDebugging(logger),
//	)
//
// Errors can opt into a specific cache duration by returning
// memoize.WithErrorTTL(err, ttl). Unwrapped errors use the configured maximum
// error TTL.
//
// Expired entries are removed by a background cleanup process. Call Close when
// a Memoizer is no longer needed to stop that process.
package memoize
