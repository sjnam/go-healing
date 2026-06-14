package healing

import (
	"context"
	"log"
	"slices"
	"time"
)

type Heartbeat int

const (
	Valid Heartbeat = iota
	Invalid
	ForceStop
)

type (
	checkHeartbeatFn func(any) Heartbeat
	StartGoroutineFn func(context.Context, time.Duration) <-chan any
)

// Default backoff bounds applied to ward restarts when WithBackoff is not used.
// The interval starts at minBackoff, doubles on each consecutive restart, and is
// capped at maxBackoff. It resets to minBackoff once a healthy heartbeat arrives.
const (
	defaultMinBackoff = 10 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

type stewardConfig struct {
	checkHeartbeat checkHeartbeatFn
	logger         *log.Logger
	minBackoff     time.Duration
	maxBackoff     time.Duration
}

// Option configures a steward created by NewSteward.
type Option func(*stewardConfig)

// WithCheckHeartbeat installs a validator that inspects each ward heartbeat. The
// ward is restarted when it returns Invalid and stopped when it returns
// ForceStop. A nil fn is ignored.
func WithCheckHeartbeat(fn func(any) Heartbeat) Option {
	return func(c *stewardConfig) {
		if fn != nil {
			c.checkHeartbeat = fn
		}
	}
}

// WithLogger directs the steward's diagnostic messages to a specific logger. Use
// log.New(io.Discard, "", 0) to silence them. A nil logger is ignored.
func WithLogger(l *log.Logger) Option {
	return func(c *stewardConfig) {
		if l != nil {
			c.logger = l
		}
	}
}

// WithBackoff sets the minimum and maximum interval the steward waits before
// restarting a failed ward. min is clamped to [0, max]; a min of 0 disables the
// initial delay. Repeated failures double the wait up to max.
func WithBackoff(min, max time.Duration) Option {
	return func(c *stewardConfig) {
		if max < 0 {
			max = 0
		}
		if min < 0 {
			min = 0
		}
		if min > max {
			min = max
		}
		c.minBackoff, c.maxBackoff = min, max
	}
}

func NewSteward(
	timeout time.Duration,
	startGoroutine StartGoroutineFn,
	opts ...Option,
) StartGoroutineFn {
	cfg := stewardConfig{
		checkHeartbeat: func(any) Heartbeat { return Valid },
		logger:         log.Default(),
		minBackoff:     defaultMinBackoff,
		maxBackoff:     defaultMaxBackoff,
	}
	for opt := range slices.Values(opts) {
		opt(&cfg)
	}

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan any {
		heartbeat := make(chan any)
		go func() {
			defer close(heartbeat)

			var (
				wardCtx       context.Context
				wardCancel    context.CancelFunc
				wardHeartbeat <-chan any
			)
			startWard := func() {
				wardCtx, wardCancel = context.WithCancel(ctx)
				wardHeartbeat = startGoroutine(wardCtx, timeout/2)
			}
			startWard()

			backoff := cfg.minBackoff
			// restart cancels the current ward, waits a backoff interval that
			// grows with consecutive failures, then starts a fresh ward. It
			// returns false if ctx is cancelled while waiting, signalling the
			// steward to stop.
			restart := func() bool {
				wardCancel()
				if backoff > 0 {
					select {
					case <-time.After(backoff):
					case <-ctx.Done():
						return false
					}
				}
				startWard()
				backoff = min(backoff*2, cfg.maxBackoff)
				return true
			}

			ticker := time.NewTicker(pulseInterval)
			defer ticker.Stop()

			for {
				timeoutSignal := time.After(timeout)
				resetTimeout := false
				for !resetTimeout {
					select {
					case <-ticker.C:
						select {
						case heartbeat <- struct{}{}:
						default:
						}
					case hb, ok := <-wardHeartbeat:
						thb := cfg.checkHeartbeat(hb)
						switch {
						case !ok || thb == Invalid:
							cfg.logger.Println("\033[31msteward: invalid heartbeat; restarting ward\033[0m")
							if !restart() {
								return
							}
						case thb == ForceStop:
							cfg.logger.Println("\033[31msteward: force stop\033[0m")
							wardCancel()
							return
						default:
							backoff = cfg.minBackoff // healthy ward; reset backoff
						}
						resetTimeout = true
					case <-timeoutSignal:
						cfg.logger.Println("\033[31msteward: ward timed out; restarting ward\033[0m")
						if !restart() {
							return
						}
						resetTimeout = true
					case <-ctx.Done():
						wardCancel()
						return
					}
				}
			}
		}()

		return heartbeat
	}
}

func or(ctxs ...context.Context) context.Context {
	switch len(ctxs) {
	case 0:
		return nil
	case 1:
		return ctxs[0]
	}

	orCtx, cancel := context.WithCancel(ctxs[0])
	go func() {
		defer cancel()
		switch len(ctxs) {
		case 2:
			select {
			case <-ctxs[0].Done():
			case <-ctxs[1].Done():
			}
		default:
			select {
			case <-ctxs[0].Done():
			case <-ctxs[1].Done():
			case <-ctxs[2].Done():
			case <-or(append(ctxs[3:], orCtx)...).Done():
			}
		}
	}()
	return orCtx
}

func orDone[T any](ctx context.Context, c <-chan T) <-chan T {
	ch := make(chan T)
	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-c:
				if !ok {
					return
				}
				select {
				case ch <- v:
				case <-ctx.Done():
				}
			}
		}
	}()

	return ch
}

func Bridge[T any](ctx context.Context, chch <-chan <-chan T) <-chan T {
	vch := make(chan T)
	go func() {
		defer close(vch)
		for {
			var ch <-chan T
			select {
			case tmp, ok := <-chch:
				if !ok {
					return
				}
				ch = tmp
			case <-ctx.Done():
				return
			}
			for val := range orDone(ctx, ch) {
				select {
				case vch <- val:
				case <-ctx.Done():
				}
			}
		}
	}()

	return vch
}
