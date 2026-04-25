package main

import (
	"context"
	"fmt"
	"math/rand"
)

func main() {
	doWork := func(ctx context.Context) (<-chan interface{}, <-chan int) {
		heartbeat := make(chan interface{}, 1)
		results := make(chan int)
		go func() {
			defer close(heartbeat)
			defer close(results)

			for i := 0; i < 10; i++ {
				select {
				case heartbeat <- struct{}{}:
				default:
				}

				select {
				case <-ctx.Done():
					return
				case results <- rand.Intn(10):
				}
			}
		}()
		return heartbeat, results
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	heartbeat, results := doWork(ctx)
	for {
		select {
		case _, ok := <-heartbeat:
			if ok {
				fmt.Println("pulse")
			} else {
				return
			}
		case r, ok := <-results:
			if ok {
				fmt.Printf("results %v\n", r)
			} else {
				return
			}
		}
	}
}
