# Agent Review Guide

This package provides a generic Go memoizer for slow external lookups. When
reviewing or changing it, keep these behavioral invariants in mind.

## Core Semantics

- There must be at most one active source call per key during its execution
  window.
- Calls for different keys may run concurrently.
- A cached successful value is returned without refresh during `minTTL`.
- After `minTTL`, `Get` starts or joins one refresh and may return stale data
  after `maxResponseTime` if the cached value is still within `maxTTL`.
- `GetFresh` starts or joins the same per-key refresh but waits for the fresh
  result until the hard timeout is reached.
- Error results are cached only when their TTL is positive. `ErrTimeout` and
  `context.Canceled` are not cached.

## Timeout And Cancellation Rules

- Every source call receives a cancelable context.
- If `hardTimeout` is positive, the call context is bounded by that timeout.
- When a call is past its execution window, cancel it before allowing a new call
  for the same key to start.
- A source that ignores context may keep its own goroutine blocked. The memoizer
  must cancel the context, stop retaining the old call record, ignore late
  results from canceled/timed-out calls, and allow a replacement call.
- Go cannot forcibly stop user code that ignores `ctx.Done()`. Do not add code
  that assumes cancellation has physically stopped an uncooperative source.

## Close Semantics

- `Close` marks the memoizer closed, cancels in-flight calls, stops background
  cleanup, and waits for in-flight source functions to return.
- After `Close`, `Get` and `GetFresh` must return `ErrClosed` and must not start
  new goroutines.
- Because `Close` waits, tests using a source that ignores context must release
  that source before deferred `Close` runs.

## Invalidation Rules

- `Delete` removes the cached entry for one key and prevents any already-running
  fetch for that key from repopulating the cache afterward.
- `Clear` removes all cached entries and prevents all already-running fetches
  from repopulating the cache afterward.
- Invalidation bookkeeping should not grow without bound. Clean up per-key
  version state once there is no entry or in-flight call for that key.

## Memory Review Checklist

- Check that `entries` cannot retain expired values after cleanup.
- Check that `calls` entries are deleted when calls finish, time out, are
  replaced, or are removed by `Cleanup`.
- Check that timers are stoppable when possible. Avoid `time.After` in hot paths
  where the timer may be abandoned before it fires.
- Check that canceled/timed-out late results cannot overwrite newer cache
  entries.
- Run both `go test ./...` and `go test -race ./...` after concurrency changes.

