package main

import (
	"context"
	"time"
)

func DoWork(
	ctx context.Context,
	nums ...int,
) (<-chan interface{}, <-chan int) {
	heartbeat := make(chan interface{}, 1)
	intStream := make(chan int)
	go func() {
		defer close(heartbeat)
		defer close(intStream)

		time.Sleep(2 * time.Second)

		for _, n := range nums {
			select {
			case heartbeat <- struct{}{}:
			default:
			}

			select {
			case <-ctx.Done():
				return
			case intStream <- n:
			}
		}
	}()
	return heartbeat, intStream
}
