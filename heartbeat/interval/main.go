package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	doWork := func(
		ctx context.Context,
		pulseInterval time.Duration,
	) (<-chan interface{}, <-chan time.Time) {
		heartbeat := make(chan interface{})
		results := make(chan time.Time)
		go func() {
			defer close(heartbeat)
			defer close(results)

			pulseTicker := time.NewTicker(pulseInterval)
			defer pulseTicker.Stop()
			workTicker := time.NewTicker(2 * pulseInterval)
			defer workTicker.Stop()
			pulse := pulseTicker.C
			workGen := workTicker.C

			sendPulse := func() {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			}
			sendResult := func(r time.Time) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-pulse:
						sendPulse()
					case results <- r:
						return
					}
				}
			}

			for {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case r := <-workGen:
					sendResult(r)
				}
			}
		}()
		return heartbeat, results
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Second, func() { cancel() })

	const timeout = 2 * time.Second
	heartbeat, results := doWork(ctx, timeout/2)
	for {
		select {
		case _, ok := <-heartbeat:
			if !ok {
				return
			}
			fmt.Println("pulse")
		case r, ok := <-results:
			if !ok {
				return
			}
			fmt.Printf("results %v\n", r.Second())
		case <-time.After(timeout):
			return
		}
	}
}
