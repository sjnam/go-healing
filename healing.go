package healing

import (
	"context"
	"log"
	"time"
)

type Heartbeat int

const (
	Valid Heartbeat = iota
	Invalid
	ForceStop
)

type (
	checkHeartbeatFn func(interface{}) Heartbeat
	StartGoroutineFn func(context.Context, time.Duration) <-chan interface{}
)

func NewSteward(
	timeout time.Duration,
	startGoroutine StartGoroutineFn,
	checkHeartbeat ...checkHeartbeatFn,
) StartGoroutineFn {
	chkhb := func(interface{}) Heartbeat { return Valid }
	if len(checkHeartbeat) == 1 {
		chkhb = checkHeartbeat[0]
	}

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan interface{} {
		heartbeat := make(chan interface{})
		go func() {
			defer close(heartbeat)

			var (
				wardCtx       context.Context
				wardCancel    context.CancelFunc
				wardHeartbeat <-chan interface{}
			)
			startWard := func() {
				wardCtx, wardCancel = context.WithCancel(ctx)
				wardHeartbeat = startGoroutine(wardCtx, timeout/2)
			}
			startWard()

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
						thb := chkhb(hb)
						if !ok || thb == Invalid {
							log.Println("steward: invalid heartbeat; restarting")
							wardCancel()
							startWard()
						} else if thb == ForceStop {
							log.Println("steward: STOP")
							wardCancel()
							return
						}
						resetTimeout = true
					case <-timeoutSignal:
						log.Println("steward: ward unhealthy; restarting")
						wardCancel()
						startWard()
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
