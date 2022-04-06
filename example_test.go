package topic_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/ostcar/topic"
)

func ExampleTopic() {
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed[string](closed))

	// Write the messages v1, v2 and v3 in a different goroutine.
	go func() {
		top.Publish("v1")
		top.Publish("v2", "v3")
		close(closed)
	}()

	// Receive the two messages and print all values.
	var id uint64
	var values []string
	var err error
	for {
		id, values, err = top.Receive(context.Background(), id)
		if err != nil {
			var closing interface {
				Closing()
			}
			if errors.As(err, &closing) {
				// topic was closed
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
