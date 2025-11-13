package topic_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/ostcar/topic"
)

func ExampleTopic() {
	ctx, shutdown := context.WithCancel(context.Background())
	top := topic.New[string]()

	// Write the messages v1, v2 and v3 in a different goroutine.
	go func() {
		top.Publish("v1")
		top.Publish("v2", "v3")
		shutdown()
	}()

	// Receive the two messages and print all values.
	var id uint64
	var values []string
	var err error
	for {
		id, values, err = top.ReceiveSince(ctx, id)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// shutdown was called.
				return
			}

			// Handle Error:
			fmt.Printf("Receive() returned an unexpected error: %v", err)
			return
		}

		// Process values:
		for _, v := range values {
			fmt.Println(v)
		}
	}

	// Unordered output:
	// v1
	// v2
	// v3
}
