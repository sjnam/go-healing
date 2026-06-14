package main

import (
	"context"
	"testing"
	"time"
)

// TestDoWork_WithoutHeartbeat verifies that results are not available within
// 1 second when the heartbeat is not awaited first, because DoWork delays 2
// seconds before producing any output. Callers must synchronize via heartbeat.
func TestDoWork_WithoutHeartbeat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intSlice := []int{0, 1, 2, 3, 5}
	_, results := DoWork(ctx, intSlice...)

	select {
	case <-results:
		t.Fatal("received result without heartbeat sync: synchronization not working")
	case <-time.After(1 * time.Second):
		// expected: work has not started yet; heartbeat sync is required
	}
}

func TestDoWork_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intSlice := []int{0, 1, 2, 3, 5}
	heartbeat, results := DoWork(ctx, intSlice...)

	<-heartbeat

	i := 0
	for r := range results {
		if expected := intSlice[i]; r != expected {
			t.Errorf("index %v: expected %v, but received %v", i, expected, r)
		}
		i++
	}
	if i != len(intSlice) {
		t.Errorf("expected %d results, got %d", len(intSlice), i)
	}
}

// TestDoWork_ContextCancel verifies that cancelling the context stops work and
// closes the results channel before all values are emitted.
func TestDoWork_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Use enough items so some are still pending when we cancel.
	intSlice := make([]int, 20)
	for i := range intSlice {
		intSlice[i] = i
	}

	heartbeat, results := DoWork(ctx, intSlice...)

	// Wait for work to start before cancelling.
	select {
	case <-heartbeat:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for initial heartbeat")
	}

	cancel()

	done := make(chan struct{})
	go func() {
		for range results {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("results channel did not close after context cancellation")
	}
}

// TestDoWork_BothChannelsCloseWhenDone verifies that both the heartbeat and
// results channels are closed once all work has been completed.
func TestDoWork_BothChannelsCloseWhenDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	intSlice := []int{0, 1, 2}
	heartbeat, results := DoWork(ctx, intSlice...)

	// Synchronize: wait for work to start.
	select {
	case <-heartbeat:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for initial heartbeat")
	}

	// Drain all results; completes when intStream is closed.
	for range results {
	}

	// Heartbeat channel should also be closed now (or have only buffered values
	// left before closing). Drain it with a timeout.
	done := make(chan struct{})
	go func() {
		for range heartbeat {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("heartbeat channel did not close after all work completed")
	}
}
