package topic_test

import (
	"context"
	"fmt"

	"github.com/ostcar/topic"
)

func ExampleTopic() {
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed(closed))

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
			// Handle Error:
			fmt.Printf("Retrive() returned an unexpected error %v", err)
			return
		}
		if len(values) == 0 {
			// When no values are returned, the topic is closed.
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
