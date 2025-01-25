package heal

import (
	"context"
	"log"
	"time"
)

type StartGoroutineFn func(context.Context, time.Duration) <-chan interface{}

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

func NewSteward(
	timeout time.Duration,
	startGoroutine StartGoroutineFn,
) StartGoroutineFn {
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
				case <-wardHeartbeat:
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
