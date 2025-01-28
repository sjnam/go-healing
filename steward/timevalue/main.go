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

			var loc *time.Location
			if tz == "" {
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

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(30*time.Second, func() { cancel() })

	doWork, tmValueStream := timeStringFn(ctx, "Asia/Seoul")
	steward := heal.NewSteward(500*time.Millisecond /*timeout*/, doWork)

	steward(ctx, time.Hour)

	for val := range tmValueStream {
		fmt.Println(val)
	}

	fmt.Println("done")
}
