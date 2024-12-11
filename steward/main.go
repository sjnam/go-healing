package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/sjnam/heal"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+[]{}\\|/.,<>;:")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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

func bridge[T any](ctx context.Context, chch <-chan <-chan T) <-chan T {
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

func randStringFn(
	ctx context.Context,
) (heal.StartGoroutineFn, <-chan string) {
	tmChanStream := make(chan (<-chan string))
	return func(ctx context.Context, pulseInterval time.Duration) <-chan interface{} {
		heartbeat := make(chan interface{})
		tmStream := make(chan string, 10)
		go func() {
			defer close(heartbeat)
			defer close(tmStream)

			select {
			case tmChanStream <- tmStream:
			case <-ctx.Done():
				return
			}

			pulse := time.Tick(pulseInterval)
			workGen := time.Tick(2 * pulseInterval)

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

			for i := 0; i < 10; i++ {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case <-workGen:
					sendResult(randSeq(28))
				}
			}
		}()
		return heartbeat
	}, bridge(ctx, tmChanStream)
	// bridge channel 덕분에 intChanStream의 공급원인 ward가 계속
	// 변하지만 intChanStream을 통해서 지속적으로 값을 보낼 수 있다.
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(30*time.Second, func() { cancel() })

	const timeout = 1 * time.Second
	doWork, stream := randStringFn(ctx)
	steward := heal.NewSteward(timeout, doWork)

	// stream을 체크하고 있기 때문에 heartbeat를 듣고 있을 필요가 없다.
	steward(ctx, 1*time.Hour)

	for val := range stream {
		log.Println(val)
	}

	fmt.Println("done")
}
