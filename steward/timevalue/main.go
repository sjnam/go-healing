package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sjnam/healing"
)

func doWorkFn(
	ctx context.Context,
	tz string,
) (healing.StartGoroutineFn, <-chan string) {
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

			pulseTicker := time.NewTicker(pulseInterval)
			defer pulseTicker.Stop()
			workTicker := time.NewTicker(1 * time.Second)
			defer workTicker.Stop()
			pulse := pulseTicker.C
			workGen := workTicker.C

			errPulse := time.After(time.Duration(1+rand.Intn(5)) * time.Second)

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
					log.Println("\033[33mward: simulating error\033[0m")
					return
				}
			}
		}()

		return heartbeat
	}, healing.Bridge(ctx, tmChanStream)
}

func checkHeartbeat(hb interface{}) healing.Heartbeat {
	tz, ok := hb.(string)
	if !ok || tz == "" {
		return healing.Invalid
	}

	log.Printf("\033[36mheartbeat: %s\033[0m", tz)

	return healing.Valid
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Second, func() {
		log.Println("\033[31mmain: halting steward and ward\033[0m")
		cancel()
	})

	var wg sync.WaitGroup

	for _, tz := range []string{
		"Asia/Seoul",
		"Asia/Singapore",
		"America/Buenos_Aires",
		"Europe/London",
		"Australia/Sydney",
		"Africa/Cairo",
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()

			doWork, stream := doWorkFn(ctx, tz)
			doWorkWithSteward := healing.NewSteward(time.Second, doWork, checkHeartbeat)
			doWorkWithSteward(ctx, time.Hour)

			city := tz[strings.LastIndex(tz, "/")+1:]
			for val := range stream {
				log.Printf("%s: %s", city, val)
			}
		}()
	}
	wg.Wait()

	log.Println("done")
}
