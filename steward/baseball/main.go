package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/sjnam/heal"
)

type pt []int

func (p pt) String() string {
	var buf strings.Builder
	for _, v := range p {
		buf.WriteString(fmt.Sprintf("%d", v))
	}
	return buf.String()
}

type result struct {
	p   pt
	cnt [2]int
}

func doWorkFn(
	ctx context.Context,
	num int,
	input <-chan pt,
) (heal.StartGoroutineFn, <-chan result) {
	tmChanStream := make(chan (<-chan result))

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan interface{} {
		heartbeat := make(chan interface{})
		tmStream := make(chan result)

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

			sendResult := func(p pt, s [2]int) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-pulse:
						sendPulse()
					case tmStream <- result{p, s}:
						return
					}
				}
			}

			target := guess(num)
			fmt.Printf("GOAL %s\n%s\n", target, strings.Repeat("=", 5+num))

			for {
				select {
				case <-ctx.Done():
					return
				case <-pulse:
					sendPulse()
				case p := <-input:
					cnt := count(target, p)
					if cnt[0] == num-1 {
						return
					}
					sendResult(p, cnt)
				}
			}
		}()

		return heartbeat
	}, heal.Bridge(ctx, tmChanStream)
}

func count(target, p pt) [2]int {
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

	return [2]int{strike, ball}
}

func guess(n int) pt {
	nums := rand.Perm(9)[:n]

	for i := 0; i < n; i++ {
		nums[i]++
	}

	return nums
}

func main() {
	ctx, cancel := context.WithCancel(context.TODO())
	time.AfterFunc(time.Hour, func() {
		fmt.Println("\033[31mmain: halting steward and ward\033[0m")
		cancel()
	})

	pitch := func(ctx context.Context, n int) <-chan pt {
		ch := make(chan pt)

		go func() {
			defer close(ch)

			for {
				select {
				case <-ctx.Done():
					return
				case ch <- guess(n):
				}
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			}
		}()

		return ch
	}

	const num = 3
	doWork, stream := doWorkFn(ctx, num, pitch(ctx, num))
	doWorkWithSteward := heal.NewSteward(500*time.Millisecond, doWork)
	doWorkWithSteward(ctx, time.Hour)

	for res := range stream {
		fmt.Printf("%s %dS%dB\n", res.p, res.cnt[0], res.cnt[1])
	}

	fmt.Println("done")
}
