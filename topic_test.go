package topic_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/ostcar/topic"
)

func TestPublishReceive(t *testing.T) {
	for _, tt := range []struct {
		name      string
		f         func(*topic.Topic[string])
		receiveID uint64
		expect    []string
	}{
		{
			"Publish two values at once",
			func(top *topic.Topic[string]) {
				top.Publish("v1", "v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Publish two values one by one",
			func(top *topic.Topic[string]) {
				top.Publish("v1")
				top.Publish("v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Publish same value twice",
			func(top *topic.Topic[string]) {
				top.Publish("v1")
				top.Publish("v1")
			},
			0,
			values("v1", "v1"),
		},
		{
			"Receive only second value",
			func(top *topic.Topic[string]) {
				top.Publish("v1")
				top.Publish("v2")
			},
			1,
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New[string]()
			tt.f(top)

			_, got, err := top.Receive(t.Context(), tt.receiveID)

			if err != nil {
				t.Errorf("Did not expect an error, got: %v", err)
			}
			if !cmpSlice(got, tt.expect) {
				t.Errorf("Got %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestPrune(t *testing.T) {
	for _, tt := range []struct {
		name      string
		f         func(*topic.Topic[string]) time.Time
		receiveID uint64
		expect    []string
	}{
		{
			"Prune after two values",
			func(top *topic.Topic[string]) time.Time {
				top.Publish("v1")
				top.Publish("v2")
				pruneTime := time.Now()
				top.Publish("v3")
				top.Publish("v4")
				return pruneTime
			},
			0,
			values("v3", "v4"),
		},
		{
			"Prune on topic with one element",
			func(top *topic.Topic[string]) time.Time {
				top.Publish("v1")
				return time.Now()
			},
			0,
			values(),
		},
		{
			"Do not prune last element",
			func(top *topic.Topic[string]) time.Time {
				top.Publish("v1")
				t := time.Now()
				top.Publish("v2")
				return t
			},
			0,
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New[string]()
			pruneTime := tt.f(top)

			top.Prune(pruneTime)

			_, got, err := top.Receive(t.Context(), 0)
			if err != nil {
				t.Fatalf("Receive(): %v", err)
			}

			if !cmpSlice(got, tt.expect) {
				t.Errorf("Got %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestPruneEmptyTopic(t *testing.T) {
	top := topic.New[string]()

	top.Prune(time.Now())

	top.Publish("foo")
	_, got, err := top.Receive(t.Context(), 0)
	if err != nil {
		t.Fatalf("Receive(): %v", err)
	}

	if !cmpSlice(got, []string{"foo"}) {
		t.Errorf("Got %v, expect [foo]", got)
	}
}

func TestErrUnknownID(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")
	ti := time.Now()
	top.Publish("v3")
	top.Publish("v4")

	top.Prune(ti)

	_, _, err := top.Receive(t.Context(), 1)
	topicErr, ok := err.(topic.UnknownIDError)
	if !ok {
		t.Errorf("Expected err to be a topic.ErrUnknownID, got: %v", err)
	}
	if topicErr.FirstID != 3 {
		t.Errorf("Expected the first id in the error to be 3, got: %d", topicErr.FirstID)
	}
	if topicErr.ID != 1 {
		t.Errorf("Expected the id in the topic to be 1, got: %d", topicErr.ID)
	}
	expect := "id 1 is unknown in the topic. Lowest id is 3"
	if got := topicErr.Error(); got != expect {
		t.Errorf("Got error message \"%s\", expected: \"%s\"", got, expect)
	}
}

func TestLastID(t *testing.T) {
	for _, tt := range []struct {
		name   string
		f      func(*topic.Topic[string])
		expect uint64
	}{
		{
			"Different values",
			func(top *topic.Topic[string]) {
				top.Publish("v1")
				top.Publish("v2")
				top.Publish("v3")
			},
			3,
		},
		{
			"Empty Topic",
			func(top *topic.Topic[string]) {},
			0,
		},
		{
			"Publish no value",
			func(top *topic.Topic[string]) {
				top.Publish()
			},
			0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New[string]()
			tt.f(top)

			got := top.LastID()

			if got != tt.expect {
				t.Errorf("LastID() == %d, expected %d", got, tt.expect)
			}

		})
	}
}

func TestReceiveBlocking(t *testing.T) {
	// Tests, that Receive() blocks until there is new data.
	top := topic.New[string]()

	// Publish a value after a short time.
	go func() {
		time.Sleep(time.Millisecond)
		top.Publish("value")
	}()

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(t.Context(), 0)
		if err != nil {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Receive() should return before the timer is over.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-received:
		if !cmpSlice(got, values("value")) {
			t.Errorf("Receive() returned %v, expected [value]", got)
		}
	case <-timer.C:
		t.Errorf("Receive() blocked for more then 100 Milliseconds, expected to get data.")
	}
}

func TestBlockUntilContexDone(t *testing.T) {
	// Tests, that Receive() unblocks, when the context is canceled
	top := topic.New[string]()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(ctx, 0)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Receive() should not return before the timer.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-received:
		t.Errorf("Receive() returned before the context was canceled.")
	case <-timer.C:
		// Cancel the context after some time.
		cancel()
	}

	// Receive should return after context was canceled.
	timer.Reset(100 * time.Millisecond)
	select {
	case got := <-received:
		if !cmpSlice(got, values()) {
			t.Errorf("Receive() returned %v, expected []", got)
		}
	case <-timer.C:
		t.Errorf("Receive() blocked for emore then 100 Milliseconds, expected to unblock after the context was canceled.")
	}
}

func TestBlockOnHighestID(t *testing.T) {
	// Test, that Receive() blocks on a non empty topic when the highest id is requested.
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")
	highestID := top.Publish("v3")

	// Close done channel, after Receive() unblocks.
	done := make(chan struct{})
	go func() {
		if _, _, err := top.Receive(t.Context(), highestID); err != nil {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		close(done)
	}()

	// Receive should not return before the timer
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
		t.Errorf("Receive() returned before the done-channel was closed.")
	case <-timer.C:
		// Publish another value, this should unblock the topic.
		top.Publish("v4")
	}

	// Receive should return after context was canceled.
	timer.Reset(100 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Errorf("Receive() blocked for emore then 100 Milliseconds, expected to unblock after another value was published.")
	}
}

func TestReceiveOnCanceledChannel(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	highestID := top.Publish("v2")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When context is canceled, Receive(0) should still return its data.
	_, got, err := top.Receive(ctx, 0)
	if err != nil {
		t.Errorf("Receive() returned the unexpected error %v", err)
	}
	if !cmpSlice(got, values("v1", "v2")) {
		t.Errorf("Receive() returned %v, expected [v1 v2]", got)
	}

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(ctx, highestID+100)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Receive() should return immediately.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-received:
		if got != nil {
			t.Errorf("Receive() returned %v, expected nil", got)
		}
	case <-timer.C:
		t.Errorf("Receive() blocked. Expect it to return immediately after context is done.")
	}
}

func TestTopicWithStruct(t *testing.T) {
	type myType struct {
		number int
		str    string
	}
	top := topic.New[myType]()
	top.Publish(myType{5, "foobar"})
	top.Publish(myType{5, "foobar"})

	_, values, err := top.Receive(t.Context(), 0)
	if err != nil {
		t.Errorf("receive: %v", err)
	}

	expect := myType{5, "foobar"}
	if len(values) != 2 || values[0] != expect {
		t.Errorf("got %v, expected [%v]", values, expect)
	}
}

func TestTopicWithPointer(t *testing.T) {
	type myType struct {
		number int
		str    string
	}
	top := topic.New[*myType]()
	top.Publish(&myType{5, "foobar"})
	top.Publish(&myType{5, "foobar"})

	_, values, err := top.Receive(t.Context(), 0)
	if err != nil {
		t.Errorf("receive: %v", err)
	}

	expect := myType{5, "foobar"}
	if len(values) != 2 || *values[0] != expect || *values[1] != expect {
		t.Errorf("got %v, expected [%v %v]", values, expect, expect)
	}
}

func cmpSlice(one, two []string) bool {
	if len(one) != len(two) {
		return false
	}

	if len(one) == 0 && len(two) == 0 {
		return true
	}

	sort.Strings(one)
	sort.Strings(two)
	for i := range one {
		if one[i] != two[i] {
			return false
		}
	}
	return true
}

func values(vs ...string) []string {
	return vs
}
