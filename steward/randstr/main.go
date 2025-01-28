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

func randStringFn(ctx context.Context) (heal.StartGoroutineFn, <-chan string) {
	tmChanStream := make(chan (<-chan string))

	return func(ctx context.Context, pulseInterval time.Duration) <-chan interface{} {
		heartbeat := make(chan interface{})
		tmStream := make(chan string)

		go func() {
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
	}, heal.Bridge(ctx, tmChanStream)
	// Thanks to the bridge channel, we can continue to send values through the tmChanStream
	// even if the ward that is the source of the tmChanStream keeps changing.
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(30*time.Second, func() { cancel() })

	doWork, stream := randStringFn(ctx)
	doWorkWithSteward := heal.NewSteward(time.Second /*timeout*/, doWork)
	// We don't need to listen to the heartbeat because we're checking the stream.
	doWorkWithSteward(ctx, time.Hour)

	for val := range stream {
		log.Println(val)
	}

	fmt.Println("done")
}
