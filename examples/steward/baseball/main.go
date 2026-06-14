package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/sjnam/healing"
)

type Pitcher []int

func (p Pitcher) String() string {
	var buf strings.Builder
	for v := range slices.Values(p) {
		fmt.Fprintf(&buf, "%d", v)
	}
	return buf.String()
}

type Result struct {
	p   Pitcher
	cnt [2]int
}

func (r Result) String() string {
	return fmt.Sprintf("%s %dS%dB", r.p, r.cnt[0], r.cnt[1])
}

func doWorkFn(
	ctx context.Context,
	num int,
	input <-chan Pitcher,
) (healing.StartGoroutineFn, <-chan Result) {
	tmChanStream := make(chan (<-chan Result))

	return func(
		ctx context.Context,
		pulseInterval time.Duration,
	) <-chan any {
		heartbeat := make(chan any)
		tmStream := make(chan Result)

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
			pulse := pulseTicker.C

			sendPulse := func() {
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			}

			sendResult := func(p Pitcher, s [2]int) {
				for {
					select {
					case <-ctx.Done():
						return
					case <-pulse:
						sendPulse()
					case tmStream <- Result{p, s}:
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
					sendResult(p, cnt)
					if cnt[0] == num {
						return
					}
				}
			}
		}()

		return heartbeat
	}, healing.Bridge(ctx, tmChanStream)
}

func count(target, p Pitcher) [2]int {
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

func guess(n int) Pitcher {
	nums := rand.Perm(9)[:n]

	for i := range n {
		nums[i]++
	}

	return nums
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(time.Hour, func() {
		fmt.Println("\033[31mmain: halting steward and ward\033[0m")
		cancel()
	})

	pitch := func(ctx context.Context, n int) <-chan Pitcher {
		ch := make(chan Pitcher)

		go func() {
			defer close(ch)

			for {
				select {
				case <-ctx.Done():
					return
				case ch <- guess(n):
				}
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			}
		}()

		return ch
	}

	num := 3
	if len(os.Args) > 1 {
		if n, err := strconv.Atoi(os.Args[1]); err == nil {
			if n < 1 || n > 9 {
				fmt.Fprintln(os.Stderr, "num must be between 1 and 9")
				os.Exit(1)
			}
			num = n
		}
	}

	doWork, stream := doWorkFn(ctx, num, pitch(ctx, num))
	doWorkWithSteward := healing.NewSteward(100*time.Millisecond, doWork)
	doWorkWithSteward(ctx, time.Hour)

	for res := range stream {
		fmt.Println(res)
	}

	fmt.Println("done")
}
