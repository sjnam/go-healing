package healing

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ExampleNewSteward demonstrates wrapping an unreliable worker goroutine with
// a steward that automatically restarts it on failure.
func ExampleNewSteward() {
	ctx, cancel := context.WithCancel(context.Background())

	time.AfterFunc(5*time.Second, func() {
		fmt.Println("main: halting steward and ward.")
		cancel()
	})

	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		fmt.Println("ward: Hello, I'm irresponsible!")
		go func() {
			<-ctx.Done()
			fmt.Println("ward: I am halting.")
		}()
		return nil
	}
	doWorkWithSteward := NewSteward(2*time.Second, doWork)
	for range doWorkWithSteward(ctx, 4*time.Second) {
	}

	fmt.Println("Done")
	// Output ordering between "I am halting" and the next restart is
	// non-deterministic due to goroutine scheduling, so this example
	// serves as documentation only.
}

// healthyWard returns a ward that sends heartbeats at pulseInterval until ctx
// is cancelled. Used as a reusable helper across steward tests.
func healthyWard(ctx context.Context, pulseInterval time.Duration) <-chan any {
	heartbeat := make(chan any)
	go func() {
		defer close(heartbeat)
		ticker := time.NewTicker(pulseInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case heartbeat <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return heartbeat
}

// --- NewSteward ---

func TestNewSteward_RestartsOnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var restarts atomic.Int32
	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		restarts.Add(1)
		return nil // nil channel → steward always times out
	}

	NewSteward(30*time.Millisecond, doWork)(ctx, time.Hour)

	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := restarts.Load(); n < 3 {
		t.Errorf("expected ≥3 restarts on timeout, got %d", n)
	}
}

func TestNewSteward_RestartsOnChannelClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var restarts atomic.Int32
	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		restarts.Add(1)
		hb := make(chan any)
		close(hb) // immediate close triggers restart
		return hb
	}

	NewSteward(time.Second, doWork)(ctx, time.Hour)

	time.Sleep(100 * time.Millisecond)
	cancel()

	if n := restarts.Load(); n < 3 {
		t.Errorf("expected ≥3 restarts on channel close, got %d", n)
	}
}

func TestNewSteward_RestartsOnInvalidHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var restarts atomic.Int32
	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		restarts.Add(1)
		hb := make(chan any, 1)
		hb <- struct{}{} // checkHeartbeat will mark this Invalid
		return hb
	}
	alwaysInvalid := func(any) Heartbeat { return Invalid }

	NewSteward(time.Second, doWork, WithCheckHeartbeat(alwaysInvalid))(ctx, time.Hour)

	time.Sleep(100 * time.Millisecond)
	cancel()

	if n := restarts.Load(); n < 3 {
		t.Errorf("expected ≥3 restarts on invalid heartbeat, got %d", n)
	}
}

func TestNewSteward_BackoffThrottlesRestarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var restarts atomic.Int32
	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		restarts.Add(1)
		hb := make(chan any)
		close(hb) // fail immediately on every start
		return hb
	}

	// A fixed 20ms backoff caps restarts to roughly window/backoff. Without
	// throttling an immediately-failing ward would restart thousands of times.
	NewSteward(time.Second, doWork, WithBackoff(20*time.Millisecond, 20*time.Millisecond))(ctx, time.Hour)

	time.Sleep(100 * time.Millisecond)
	cancel()

	// ~1 initial start + ~5 restarts in 100ms. Allow generous slack for
	// scheduling but assert the storm is bounded well below an unthrottled loop.
	if n := restarts.Load(); n > 15 {
		t.Errorf("expected backoff to throttle restarts to a handful, got %d", n)
	}
	if n := restarts.Load(); n < 2 {
		t.Errorf("expected at least one restart within the window, got %d", n)
	}
}

func TestNewSteward_ForceStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doWork := func(ctx context.Context, _ time.Duration) <-chan any {
		hb := make(chan any, 1)
		hb <- ForceStop
		return hb
	}
	// checkHeartbeat must forward Heartbeat typed values so ForceStop propagates.
	passThrough := func(hb any) Heartbeat {
		if h, ok := hb.(Heartbeat); ok {
			return h
		}
		return Valid
	}

	hbCh := NewSteward(time.Second, doWork, WithCheckHeartbeat(passThrough))(ctx, time.Hour)

	select {
	case _, ok := <-hbCh:
		if ok {
			for range hbCh {
			} // drain residual pulses
			t.Error("expected heartbeat channel to be closed after ForceStop")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("steward did not stop after ForceStop")
	}
}

func TestNewSteward_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hbCh := NewSteward(time.Second, healthyWard)(ctx, 20*time.Millisecond)

	// Confirm the steward is running before we cancel.
	select {
	case <-hbCh:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("steward did not emit a heartbeat before cancel")
	}

	cancel()

	done := make(chan struct{})
	go func() {
		for range hbCh {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("steward heartbeat channel did not close after context cancellation")
	}
}

func TestNewSteward_EmitsHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hbCh := NewSteward(time.Second, healthyWard)(ctx, 20*time.Millisecond)

	count := 0
	deadline := time.After(300 * time.Millisecond)
	for count < 3 {
		select {
		case <-hbCh:
			count++
		case <-deadline:
			t.Fatalf("expected ≥3 steward heartbeats within 300ms, got %d", count)
		}
	}
}

// --- or ---

func TestOr_TwoContexts(t *testing.T) {
	t.Run("cancels when first is done", func(t *testing.T) {
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel2()

		combined := or(ctx1, ctx2)
		cancel1()
		select {
		case <-combined.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("combined context did not cancel when ctx1 was cancelled")
		}
	})

	t.Run("cancels when second is done", func(t *testing.T) {
		ctx1, cancel1 := context.WithCancel(context.Background())
		ctx2, cancel2 := context.WithCancel(context.Background())
		defer cancel1()

		combined := or(ctx1, ctx2)
		cancel2()
		select {
		case <-combined.Done():
		case <-time.After(100 * time.Millisecond):
			t.Fatal("combined context did not cancel when ctx2 was cancelled")
		}
	})
}

func TestOr_ManyContexts(t *testing.T) {
	ctxs := make([]context.Context, 5)
	cancels := make([]context.CancelFunc, 5)
	for i := range ctxs {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
	}
	defer func() {
		for _, c := range cancels {
			c()
		}
	}()

	combined := or(ctxs...)
	cancels[3]() // cancel only the 4th context

	select {
	case <-combined.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("combined context did not cancel when one of the contexts was cancelled")
	}
}

func TestOr_SingleContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if result := or(ctx); result != ctx {
		t.Fatal("or with a single context should return that same context")
	}
}

// --- orDone ---

func TestOrDone_PassesAllValues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	var got []int
	for v := range orDone(ctx, ch) {
		got = append(got, v)
	}

	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("expected [1 2 3], got %v", got)
	}
}

func TestOrDone_ClosesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	out := orDone(ctx, make(chan int)) // source never sends
	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected closed channel after context cancellation")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("orDone did not close after context cancellation")
	}
}

// --- Bridge ---

func TestBridge_FlattensChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chch := make(chan (<-chan int), 2)

	ch1 := make(chan int, 2)
	ch1 <- 1
	ch1 <- 2
	close(ch1)

	ch2 := make(chan int, 2)
	ch2 <- 3
	ch2 <- 4
	close(ch2)

	chch <- ch1
	chch <- ch2
	close(chch)

	var got []int
	for v := range Bridge(ctx, chch) {
		got = append(got, v)
	}

	if len(got) != 4 {
		t.Errorf("expected 4 values, got %d: %v", len(got), got)
	}
	// Values arrive in order within each inner channel.
	if got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
		t.Errorf("unexpected order: %v", got)
	}
}

func TestBridge_ClosesOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	out := Bridge(ctx, make(chan (<-chan int))) // source never sends
	cancel()

	select {
	case _, ok := <-out:
		if ok {
			t.Fatal("expected closed channel after context cancellation")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Bridge did not close after context cancellation")
	}
}
