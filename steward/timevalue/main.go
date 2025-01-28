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

func timeStringFn(ctx context.Context, tz string) (heal.StartGoroutineFn, <-chan string) {
	tmChanStream := make(chan (<-chan string))
	return func(ctx context.Context, pulseInterval time.Duration) <-chan interface{} {
		heartbeat := make(chan interface{})
		tmStream := make(chan string, 10)
		go func() {
			defer close(tmStream)

			select {
			case tmChanStream <- tmStream:
			case <-ctx.Done():
				return
			}

			pulse := time.Tick(pulseInterval)
			workGen := time.Tick(1 * time.Second)

			errPulse := time.After(time.Duration(rand.Intn(20)) * time.Second)

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

			loc, err := time.LoadLocation(tz)
			if err != nil {
				panic(err)
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
	// bridge channel 덕분에 tmChanStream의 공급원인 ward가 계속
	// 변하지만 tmChanStream을 통해서 지속적으로 값을 보낼 수 있다.
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(30*time.Second, func() { cancel() })

	const timeout = 500 * time.Millisecond
	doWork, stream := timeStringFn(ctx, "Asia/Singapore")
	steward := heal.NewSteward(timeout, doWork)

	// stream을 체크하고 있기 때문에 heartbeat를 듣고 있을 필요가 없다.
	steward(ctx, 1*time.Hour)

	for val := range stream {
		fmt.Println(val)
	}

	fmt.Println("done")
}
