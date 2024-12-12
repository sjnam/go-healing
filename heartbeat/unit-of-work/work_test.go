package main

import (
	"context"
	"testing"
	"time"
)

func TestDoWork_Fail(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	intSlice := []int{0, 1, 2, 3, 5}
	_, results := DoWork(ctx, intSlice...)

	for i, expected := range intSlice {
		select {
		case r := <-results:
			if r != expected {
				t.Errorf(
					"index %v: expected %v, but received %v",
					i,
					expected,
					r,
				)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("test time out")
		}
	}
}

func TestDoWork_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
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
}
