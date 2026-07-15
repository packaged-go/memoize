# memoize

Typed, concurrency-safe memoization for slow external lookups in Go.

A `Memoizer` caches successful responses per key, coalesces concurrent
refreshes so only one source call per key is ever in flight, and can serve
stale data while a refresh runs in the background — a stale-while-revalidate
cache for function calls.

```bash
go get github.com/packaged-go/memoize
```

Requires Go 1.22+.

## Quick start

```go
func loadUser(ctx context.Context, id string) (User, error) {
    // slow lookup: database, HTTP API, gRPC call, ...
}

cache := memoize.NewKeyed[string, User](loadUser)
defer cache.Close()

user, err := cache.Get("user:123")
```

The first `Get` for a key calls the source and caches the result. Subsequent
calls return the cached value instantly while it is fresh, and coordinate a
single shared refresh once it isn't.

## The value lifecycle

Every successful fetch stamps the entry with `fetchedAt`. Two TTLs then govern
what happens on each `Get`:

```
fetchedAt          fetchedAt + minTTL                    fetchedAt + maxTTL
    |------ FRESH ------|------------- STALE -------------------|-- GONE --
    | return cached     | start/join one refresh;               | block on
    | value immediately | wait up to maxResponseTime, then      | refresh
    |                   | fall back to the stale value          |
```

- **Within `minTTL`** — the cached value is returned immediately. No source
  call happens.
- **Between `minTTL` and `maxTTL`** — `Get` starts a refresh (or joins one
  already running for that key) and waits up to `maxResponseTime` for it. If
  the refresh doesn't finish in time, the stale cached value is returned and
  the refresh keeps running in the background; the next caller sees the fresh
  result.
- **Past `maxTTL`** (or never cached) — there is no acceptable value, so `Get`
  blocks on the refresh until it completes or the `hardTimeout` expires, in
  which case it returns `ErrTimeout`.

`GetFresh` follows the same rules except it always waits for the in-flight
refresh rather than settling for a stale value at `maxResponseTime`. It only
falls back to the stale value if the `hardTimeout` is reached first.

### Request coalescing

At most one source call per key is active at a time. Any number of goroutines
calling `Get`/`GetFresh` for the same key share that single call and all
receive its result. Different keys refresh independently and concurrently.

### Hard timeout

`hardTimeout` bounds each source call's context. When it expires the context
is cancelled, waiters receive `ErrTimeout` (or the stale value when one is
available), and a replacement call is allowed to start. A source that ignores
`ctx.Done()` can't be forcibly stopped — its late result is discarded rather
than cached — so make your source functions context-aware.

## Configuration

Options are passed to `New`/`NewKeyed`:

```go
cache := memoize.NewKeyed[string, User](loadUser,
    memoize.WithMinimumTTL(30*time.Second),
    memoize.WithMaximumTTL(5*time.Minute),
    memoize.WithMaximumResponseTime(50*time.Millisecond),
    memoize.WithHardTimeout(2*time.Second),
)
```

| Option | Default | Meaning |
| --- | --- | --- |
| `WithMinimumTTL` | `5s` | Period after a successful fetch where the cached value is returned without any refresh attempt. |
| `WithMaximumTTL` | `1h` | Hard lifetime for successful cached values. Never lower than the minimum TTL (it is raised to match if configured lower). |
| `WithMaximumResponseTime` | `300ms` | How long `Get` waits for a refresh before returning a stale value. Non-positive means stale values are returned immediately while a background refresh runs. |
| `WithHardTimeout` | `10s` | Maximum time a source call may run before its context is cancelled. Non-positive disables the bound. |
| `WithMaximumErrorTTL` | `1m` | Default and upper bound TTL for cached errors (see below). |
| `WithCleanupInterval` | `10m` | How often the background goroutine evicts expired entries. Non-positive disables background cleanup; call `Cleanup()` manually if needed. |
| `WithDebugging` | off | Attach a `*zap.Logger` for debug-level trace events. |

## Params

Sources can accept a caller-defined params value alongside the key:

```go
type LookupParams struct {
    Region string
}

cache := memoize.New[string, User, LookupParams](loadUser)
user, err := cache.Get("user:123", LookupParams{Region: "eu"})
```

Params are variadic at the call site; when omitted, the source receives the
zero value of `P` (`nil` for pointer/interface types). Note that **params are
not part of the cache key** — callers joining an in-flight refresh share the
result regardless of the params they passed, and only the params of the caller
that *started* the call reach the source. Encode anything that should produce
a distinct cached value into the key itself.

For ad-hoc params without defining a struct, the package provides a generic
`memoize.Params` bag:

```go
cache := memoize.New[string, User, memoize.Params](loadUser)
user, err := cache.Get("user:123") // source receives zero Params
```

## Error caching

Failed fetches can be cached to shield a struggling backend from a stampede of
retries:

- By default, errors are cached for `WithMaximumErrorTTL` (1 minute).
- A source can choose a specific duration per error with
  `memoize.WithErrorTTL(err, ttl)`. The configured maximum error TTL still
  caps it. A non-positive TTL disables caching for that error.
- `ErrTimeout` and `context.Canceled` results are never cached.

```go
func loadUser(ctx context.Context, id string) (User, error) {
    u, err := api.FetchUser(ctx, id)
    if errors.Is(err, api.ErrUserNotFound) {
        // negative-cache "not found" briefly
        return User{}, memoize.WithErrorTTL(err, 10*time.Second)
    }
    return u, err
}
```

While a cached error is live, `Get` returns it without calling the source.

## Cache control

| Method | Behavior |
| --- | --- |
| `Delete(key)` | Removes the entry for a key. An already-running fetch for that key completes but will **not** repopulate the cache. |
| `Clear()` | Removes all entries. In-flight fetches complete but do not repopulate. |
| `Extend(key, ttl)` | Pushes a cached key's expiry to `now + ttl`. Returns `ErrNotFound` if the key isn't cached. |
| `Len()` | Number of entries currently held in memory (including expired ones not yet cleaned up). |
| `Cleanup()` | Immediately evicts expired entries and timed-out call records; returns the count removed. Normally handled by the background loop. |
| `Close()` | Cancels in-flight calls, stops background cleanup, and **waits** for source functions to return. Afterwards `Get`/`GetFresh` return `ErrClosed`. |

Call `Close` when a memoizer is no longer needed — it owns a background
goroutine when the cleanup interval is positive.

## Errors

| Error | When |
| --- | --- |
| `ErrTimeout` | A fetch could not complete before the hard timeout and no stale value was available. |
| `ErrNotFound` | `Extend` was called for a key that isn't cached. |
| `ErrClosed` | `Get`/`GetFresh` called after `Close`. |

All other errors come from your source function (possibly served from the
error cache).

## Debugging

Pass a zap logger to trace every state transition — cache hits and misses,
refresh starts/joins, stale returns, source call results, invalidation, and
cleanup:

```go
logger, _ := zap.NewDevelopment()
cache := memoize.NewKeyed[string, User](loadUser,
    memoize.WithDebugging(logger),
)
```

Events are logged at debug level with the key, TTL configuration, and timing
attached as structured fields (all under the `memoize.*` message namespace).

## Development

```bash
go test ./...        # unit tests
go test -race ./...  # required after any concurrency change
```

`memoize_stress_test.go` hammers the memoizer with concurrent readers,
invalidation, and misbehaving sources. See [AGENTS.md](AGENTS.md) for the
behavioral invariants the implementation must preserve — single active call
per key, cancellation rules, close semantics, and the memory-leak review
checklist.
