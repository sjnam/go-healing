package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/sjnam/healing"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+[]{}\\|/.,<>;:")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func randStringFn(ctx context.Context) (healing.StartGoroutineFn, <-chan string) {
	tmChanStream := make(chan (<-chan string))

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan any {
		heartbeat := make(chan any)
		tmStream := make(chan string)

		go func() {
			defer close(tmStream)
			defer close(heartbeat)

			select {
			case tmChanStream <- tmStream:
			case <-ctx.Done():
				return
			}

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

			for range 10 {
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
	}, healing.Bridge(ctx, tmChanStream)
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Second, func() {
		log.Println("\033[31mmain: halting steward and ward\033[0m")
		cancel()
	})

	doWork, stream := randStringFn(ctx)
	doWorkWithSteward := healing.NewSteward(time.Second /*timeout*/, doWork)
	doWorkWithSteward(ctx, time.Hour)

	for val := range stream {
		log.Println(val)
	}

	log.Println("done")
}
