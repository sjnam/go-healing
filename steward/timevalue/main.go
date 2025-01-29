package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/sjnam/heal"
)

func doWorkFn(ctx context.Context, tz string) (heal.StartGoroutineFn, <-chan string) {
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
			workGen := time.Tick(1 * time.Second)

			errPulse := time.After(time.Duration(1+rand.Intn(10)) * time.Second)

			sendPulse := func() {
				select {
				case heartbeat <- tz:
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

			var loc *time.Location
			if tz != "" {
				loc, _ = time.LoadLocation(tz)
			} else {
				loc = time.Local
			}

			for {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case <-workGen:
					sendResult(time.Now().In(loc).Format(time.RFC3339))
				case <-errPulse:
					return
				}
			}
		}()

		return heartbeat
	}, heal.Bridge(ctx, tmChanStream)
	// Thanks to the bridge channel, we can continue to send values through the tmChanStream
	// even if the ward that is the source of the tmChanStream keeps changing.
}

func checkHeartbeat(hb interface{}) bool {
	tz := hb.(string)
	if tz == "" {
		return false
	}

	log.Printf("heartbeat: %s\n", tz)

	return true
}

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(30*time.Second, func() {
		log.Println("main: halting steward and ward.")
		cancel()
	})

	var wg sync.WaitGroup
	for _, tz := range []string{"Asia/Seoul", "Asia/Singapore", "America/Buenos_Aires", "Europe/London", "Australia/Sydney", "Africa/Cairo"} {
		wg.Add(1)
		go func(tz string) {
			defer wg.Done()

			doWork, stream := doWorkFn(ctx, tz)
			doWorkWithSteward := heal.NewSteward(time.Second /*timeout*/, doWork, checkHeartbeat)
			doWorkWithSteward(ctx, time.Hour)

			city := tz[strings.LastIndex(tz, "/")+1:]
			for val := range stream {
				fmt.Printf("%s:\t%s\n", city, val)
			}
		}(tz)
	}
	wg.Wait()

	fmt.Println("done")
}
