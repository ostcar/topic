package topic_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ostcar/topic"
)

func TestPublishReceive(t *testing.T) {
	for _, tt := range []struct {
		name      string
		f         func(*topic.Topic)
		receiveID uint64
		expect    []string
	}{
		{
			"Publish two values at once",
			func(top *topic.Topic) {
				top.Publish("v1", "v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Publish two values one by one",
			func(top *topic.Topic) {
				top.Publish("v1")
				top.Publish("v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Publish same value twice",
			func(top *topic.Topic) {
				top.Publish("v1")
				top.Publish("v1")
			},
			0,
			values("v1"),
		},
		{
			"Receive only second value",
			func(top *topic.Topic) {
				top.Publish("v1")
				top.Publish("v2")
			},
			1,
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New()
			tt.f(top)

			_, got, err := top.Receive(context.Background(), tt.receiveID)

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
		f         func(*topic.Topic) time.Time
		receiveID uint64
		expect    []string
	}{
		{
			"Prune after two values",
			func(top *topic.Topic) time.Time {
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
			"Prune on empty topic",
			func(top *topic.Topic) time.Time {
				return time.Now()
			},
			0,
			values(),
		},
		{
			"Prune on topic with one element",
			func(top *topic.Topic) time.Time {
				top.Publish("v1")
				return time.Now()
			},
			0,
			values("v1"),
		},
		{
			"Do not prune last element",
			func(top *topic.Topic) time.Time {
				top.Publish("v1")
				top.Publish("v2")
				return time.Now()
			},
			0,
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			closed := make(chan struct{})
			close(closed)
			top := topic.New(topic.WithClosed(closed))
			pruneTime := tt.f(top)

			top.Prune(pruneTime)

			_, got, err := top.Receive(context.Background(), 0)
			if err != nil {
				t.Errorf("Receive() returned an unexpected error %v", err)
			}
			if !cmpSlice(got, tt.expect) {
				t.Errorf("Got %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestErrUnknownID(t *testing.T) {
	top := topic.New()
	top.Publish("v1")
	top.Publish("v2")
	ti := time.Now()
	top.Publish("v3")
	top.Publish("v4")

	top.Prune(ti)

	_, _, err := top.Receive(context.Background(), 1)
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
		f      func(*topic.Topic)
		expect uint64
	}{
		{
			"Different values",
			func(top *topic.Topic) {
				top.Publish("v1")
				top.Publish("v2")
				top.Publish("v3")
			},
			3,
		},
		{
			"Empty Topic",
			func(top *topic.Topic) {},
			0,
		},
		{
			"Publish no value",
			func(top *topic.Topic) {
				top.Publish()
			},
			1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New()
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
	top := topic.New()

	// Publish a value after a short time.
	go func() {
		time.Sleep(time.Millisecond)
		top.Publish("value")
	}()

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(context.Background(), 0)
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

func TestBlockUntilClose(t *testing.T) {
	// Tests, that Receive() unblocks, when the topic is closed.
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed(closed))

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(context.Background(), 0)
		if err != nil {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Receive() should not return before the timer.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-received:
		t.Errorf("Receive() returned before the topic was closed.")
	case <-timer.C:
		// Close the topic after some time.
		close(closed)
	}

	// Receive should return after the topic was closed.
	timer.Reset(100 * time.Millisecond)
	select {
	case got := <-received:
		if !cmpSlice(got, values()) {
			t.Errorf("Receive() returned %v, expected []", got)
		}
	case <-timer.C:
		t.Errorf("Receive() blocked for emore then 100 Milliseconds, expected to unblock after topic is closed.")
	}
}

func TestBlockUntilContexDone(t *testing.T) {
	// Tests, that Receive() unblocks, when the context is canceled
	top := topic.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(ctx, 0)
		if err != nil {
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
	top := topic.New()
	top.Publish("v1")
	top.Publish("v2")
	highestID := top.Publish("v3")

	// Close done channel, after Receive() unblocks.
	done := make(chan struct{})
	go func() {
		if _, _, err := top.Receive(context.Background(), highestID); err != nil {
			t.Errorf("Receive() returned the unexpected error %v", err)
		}
		close(done)
	}()

	// Receive should not return before the timer
	timer := time.NewTimer(time.Millisecond)
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

func TestReceiveOnClosedTopic(t *testing.T) {
	// Test, that a Receive()-call on a already closed topic returnes
	// immediately with all values.
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed(closed))
	top.Publish("v1")
	highestID := top.Publish("v2")
	close(closed)

	// When the topic is closed, Receive(0) should still return its data.
	_, got, err := top.Receive(context.Background(), 0)
	if err != nil {
		t.Errorf("Receive() returned the unexpected error %v", err)
	}
	if !cmpSlice(got, values("v1", "v2")) {
		t.Errorf("Receive() returned %v, expected [v1 v2]", got)
	}

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Receive(context.Background(), highestID+100)
		if err != nil {
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
		t.Errorf("Receive() blocked. Expect it to return immediately when the topic is closed.")
	}
}

func TestReceiveOnCanceledChannel(t *testing.T) {
	top := topic.New()
	top.Publish("v1")
	highestID := top.Publish("v2")
	ctx, cancel := context.WithCancel(context.Background())
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
		if err != nil {
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
		t.Errorf("Receive() blocked. Expect it to return immediately when the topic is closed.")
	}
}

func TestWithStartID(t *testing.T) {
	top := topic.New(topic.WithStartID(100))

	t.Run("LastID", func(t *testing.T) {
		if top.LastID() != 100 {
			t.Errorf("LastID returned %d, expected 100", top.LastID())
		}
	})

	t.Run("Receive", func(t *testing.T) {
		id, value, err := top.Receive(context.Background(), 0)

		if err != nil {
			t.Errorf("Receive returned err %v", err)
		}

		if id != 100 {
			t.Errorf("Receive returned id %d, expected 100", id)
		}

		if !cmpSlice(value, []string{}) {
			t.Errorf("Receive returned values %v, expected no data", value)
		}
	})

	t.Run("Publish", func(t *testing.T) {
		id := top.Publish("value")

		if id != 101 {
			t.Errorf("Publish returned id %d, expected 101", id)
		}

		if _, got, _ := top.Receive(context.Background(), 0); !cmpSlice(got, []string{"value"}) {
			t.Errorf("Receive returned %v, expected [value]", got)
		}
	})

}

func cmpSlice(one, two []string) bool {
	if len(one) != len(two) {
		return false
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
