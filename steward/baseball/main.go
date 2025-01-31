package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sjnam/heal"
)

func doWorkFn(
	ctx context.Context,
	num int,
	balls <-chan []int,
) (heal.StartGoroutineFn, <-chan string) {
	tmChanStream := make(chan (<-chan string))

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan interface{} {
		heartbeat := make(chan interface{})
		tmStream := make(chan string)

		go func() {
			defer close(tmStream)
			defer close(heartbeat)

			select {
			case tmChanStream <- tmStream:
			case <-ctx.Done():
				return
			}

			pulse := time.Tick(pulseInterval)

			sendPulse := func() {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			}

			sendResult := func(s string) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-pulse:
						sendPulse()
					case tmStream <- s:
						return
					}
				}
			}

			target := pitch(num)
			log.Print("TARGET", target)

			for {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case p := <-balls:
					cnt := count(target, p)
					if cnt == "0S 0B" {
						return
					}
					sendResult(cnt)
				}
			}
		}()

		return heartbeat
	}, heal.Bridge(ctx, tmChanStream)
}

func count(target, p []int) string {
	var strike, ball int
	for i, v := range p {
		if target[i] == v {
			strike++
		} else {
			for j, tv := range target {
				if i != j && tv == v {
					ball++
					break
				}
			}
		}
	}

	return fmt.Sprintf("%dS %dB", strike, ball)
}

func pitch(n int) []int {
	var numbers []int
	for v := range Permgen(9, n) {
		numbers = append(numbers, v)
	}
	return numbers
}

func guess(ctx context.Context, n int) <-chan []int {
	ch := make(chan []int)
	go func() {
		defer close(ch)
		for {
			ball := pitch(n)
			log.Print(ball)
			select {
			case <-ctx.Done():
				return
			case ch <- ball:
			}
			time.Sleep(200 * time.Millisecond)
		}
	}()

	return ch
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(time.Hour, func() {
		log.Println("\033[31mmain: halting steward and ward\033[0m")
		cancel()
	})

	const num = 4
	ch := guess(ctx, num)
	doWork, stream := doWorkFn(ctx, num, ch)
	doWorkWithSteward := heal.NewSteward(100*time.Millisecond, doWork)
	doWorkWithSteward(ctx, time.Hour)

	for val := range stream {
		log.Print(val)
	}

	log.Println("done")
}
