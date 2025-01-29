package heal

import (
	"context"
	"log"
	"time"
)

type (
	checkHeartbeatFn func(interface{}) bool
	StartGoroutineFn func(context.Context, time.Duration) <-chan interface{}
)

func NewSteward(
	timeout time.Duration,
	startGoroutine StartGoroutineFn,
	checkHeartbeat ...checkHeartbeatFn,
) StartGoroutineFn {
	chkhb := func(interface{}) bool { return true }
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
				wardHeartbeat = startGoroutine(or(ctx, wardCtx), timeout/2)
			}
			startWard()
			pulse := time.Tick(pulseInterval)

		monitorLoop:
			timeoutSignal := time.After(timeout)
			for {
				select {
				case <-pulse:
					select {
					case heartbeat <- struct{}{}:
					default:
					}
				case hb, ok := <-wardHeartbeat:
					if !ok || !chkhb(hb) {
						log.Println("steward: invalid heartbeat; restarting")
						wardCancel()
						startWard()
					}
					goto monitorLoop
				case <-timeoutSignal:
					log.Println("steward: ward unhealthy; restarting")
					wardCancel()
					startWard()
					goto monitorLoop
				case <-ctx.Done():
					return
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
