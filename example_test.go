package topic_test

import (
	"context"
	"fmt"

	"github.com/ostcar/topic"
)

func ExampleTopic() {
	top := topic.New()

	// Write the messages v1, v2 and v3 in a different goroutine.
	go func() {
		top.Add("v1")
		top.Add("v2", "v3")
	}()

	// Receive the to messages and print all values
	var id uint64
	var values []string
	var err error
	for id < 2 {
		id, values, err = top.Get(context.Background(), id)
		if err != nil {
			// Handle Error:
			fmt.Printf("Get() returned an unexpected error %v", err)
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
