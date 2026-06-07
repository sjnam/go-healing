# Self-Healing Goroutines for Go

`healing` is a tiny, dependency-free library that keeps long-running
("daemon-like") goroutines alive. It implements the **Steward–Ward** pattern: a
*steward* goroutine supervises a *ward* (your worker), watches its heartbeat, and
automatically restarts it when it stalls, dies, or reports itself unhealthy.

Built on pure Go standard library — no external dependencies.

```text
caller ─▶ NewSteward(workFn, opts) ─▶ stewardFn
caller ─▶ stewardFn(ctx, pulse)    ─▶ heartbeat channel
              │
              ▼ supervises
           ward ──heartbeat──▶ steward ──restarts on failure──▶ new ward
```

## Install

```bash
go get github.com/sjnam/healing
```

Requires Go 1.23+.

## Concepts

### Steward & Ward

Both the ward and the steward share one signature, which makes them composable —
a steward can itself be wrapped by another steward:

```go
type StartGoroutineFn func(ctx context.Context, pulseInterval time.Duration) <-chan interface{}
```

A ward returns a **heartbeat channel** and is expected to send a value on it at
least every `pulseInterval`. The steward treats silence as a failure.

### Heartbeats & timeout

`NewSteward(timeout, ...)` takes a `timeout`. The ward is asked to pulse every
`timeout/2`, giving it margin. The steward restarts the ward if it does not hear
a heartbeat within `timeout`, if the heartbeat channel closes, or if a custom
validator marks a heartbeat as `Invalid`.

### Heartbeat states

A validator (see [`WithCheckHeartbeat`](#options)) classifies each heartbeat:

| State       | Meaning                                            | Effect            |
|-------------|----------------------------------------------------|-------------------|
| `Valid`     | Ward is healthy                                    | Continue          |
| `Invalid`   | Ward has failed                                    | Restart the ward  |
| `ForceStop` | Intentional shutdown requested by the ward         | Stop the steward  |

The default validator always returns `Valid`.

### Restart backoff

To avoid a restart storm when a ward fails immediately and repeatedly, restarts
are spaced by an exponential backoff: it starts at the minimum, doubles on each
consecutive failure up to the maximum, and resets to the minimum as soon as a
healthy heartbeat arrives. Defaults are `10ms → 5s`; override with
[`WithBackoff`](#options). The backoff wait is cancellation-aware — cancelling
the context stops the steward even mid-wait.

## Quick start

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/sjnam/healing"
)

// worker pulses every pulseInterval and occasionally "dies" to demonstrate
// automatic recovery.
func worker(ctx context.Context, pulseInterval time.Duration) <-chan interface{} {
	heartbeat := make(chan interface{})
	go func() {
		defer close(heartbeat)
		pulse := time.NewTicker(pulseInterval)
		defer pulse.Stop()
		die := time.After(3 * time.Second) // simulate an occasional crash

		for {
			select {
			case <-ctx.Done():
				return
			case <-pulse.C:
				select { // non-blocking send
				case heartbeat <- struct{}{}:
				default:
				}
			case <-die:
				log.Println("worker: crashing")
				return // closing the channel triggers a restart
			}
		}
	}()
	return heartbeat
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	steward := healing.NewSteward(2*time.Second, worker)

	// Drain the steward's own heartbeats; the ward is restarted under the hood.
	for range steward(ctx, time.Second) {
		log.Println("steward: alive")
	}
}
```

## Consuming results across restarts

Each ward restart produces a *new* result channel. `Bridge` flattens a stream of
channels (`<-chan <-chan T`) into a single `<-chan T`, so callers can keep
reading results seamlessly across restarts. The common idiom is for your worker
factory to publish each ward's result channel onto a channel-of-channels and
return `Bridge(ctx, that)`:

```go
func workFn(ctx context.Context) (healing.StartGoroutineFn, <-chan string) {
	chch := make(chan (<-chan string))

	startFn := func(ctx context.Context, pulse time.Duration) <-chan interface{} {
		heartbeat := make(chan interface{})
		results := make(chan string)
		go func() {
			defer close(results)
			defer close(heartbeat)
			select {
			case chch <- results: // hand this ward's results to the bridge
			case <-ctx.Done():
				return
			}
			// ... produce results, send heartbeats ...
		}()
		return heartbeat
	}

	return startFn, healing.Bridge(ctx, chch)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	work, results := workFn(ctx)
	healing.NewSteward(time.Second, work)(ctx, time.Hour)

	for r := range results { // uninterrupted across ward restarts
		log.Println(r)
	}
}
```

## API

### `NewSteward`

```go
func NewSteward(timeout time.Duration, startGoroutine StartGoroutineFn, opts ...Option) StartGoroutineFn
```

Wraps `startGoroutine` (the ward) with a supervising steward and returns a
`StartGoroutineFn`. Calling the returned function with `(ctx, pulseInterval)`
starts the steward and returns the steward's own heartbeat channel; the channel
closes when the context is cancelled or the ward force-stops.

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithCheckHeartbeat(fn func(interface{}) Heartbeat)` | Validator that classifies each ward heartbeat as `Valid`/`Invalid`/`ForceStop`. A `nil` fn is ignored. | always `Valid` |
| `WithLogger(l *log.Logger)` | Destination for the steward's diagnostic logs. Pass `log.New(io.Discard, "", 0)` to silence. A `nil` logger is ignored. | `log.Default()` |
| `WithBackoff(min, max time.Duration)` | Restart backoff bounds. `min` is clamped to `[0, max]`; a `min` of `0` disables the initial delay. | `10ms`, `5s` |

### `Bridge`

```go
func Bridge[T any](ctx context.Context, chch <-chan <-chan T) <-chan T
```

Flattens a channel of channels into a single channel, closing when `ctx` is done
or the source is exhausted. Used to consume ward results across restarts.

## Examples

Runnable programs live under [`steward/`](steward) and [`heartbeat/`](heartbeat):

```bash
go run ./steward/randstr         # random strings, ward restarts on simulated errors
go run ./steward/timevalue       # multiple concurrent stewards, custom validator, graceful shutdown
go run ./steward/baseball [num]  # number-guessing game; num = target length (1–9)
go run ./heartbeat/interval      # bare interval-based heartbeat
go run ./heartbeat/unit-of-work  # heartbeat emitted per unit of work
```

`steward/timevalue` is the most complete reference: it shows a custom
`checkHeartbeat`, several concurrent stewards, and clean cancellation.

## Testing

```bash
go test ./...            # all tests
go test -race ./...      # with the race detector
go test -v -run TestName ./...
```

## Acknowledgements

The Steward–Ward pattern and the heartbeat/`Bridge` building blocks are adapted
from Katherine Cox-Buday's *Concurrency in Go* (O'Reilly), extended here with
restart backoff and configurable logging.
