package healing

import (
	"context"
	"fmt"
	"time"
)

func ExampleNewSteward() {
	ctx, cancel := context.WithCancel(context.TODO())

	time.AfterFunc(5*time.Second, func() {
		fmt.Println("main: halting steward and ward.")
		cancel()
	})

	doWork := func(ctx context.Context, _ time.Duration) <-chan interface{} {
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

	// Output:
	// ward: Hello, I'm irresponsible!
	// ward: Hello, I'm irresponsible!
	// ward: I am halting.
	// ward: Hello, I'm irresponsible!
	// ward: I am halting.
	// main: halting steward and ward.
	// ward: I am halting.
	// Done
}
