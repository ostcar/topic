package topic_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/ostcar/topic"
)

func TestAddGet(t *testing.T) {
	for _, tt := range []struct {
		name   string
		f      func(*topic.Topic)
		getID  uint64
		expect []string
	}{
		{
			"Add two values at once",
			func(top *topic.Topic) {
				top.Add("v1", "v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Add two values one by one",
			func(top *topic.Topic) {
				top.Add("v1")
				top.Add("v2")
			},
			0,
			values("v1", "v2"),
		},
		{
			"Add same value twice",
			func(top *topic.Topic) {
				top.Add("v1")
				top.Add("v1")
			},
			0,
			values("v1"),
		},
		{
			"Get second value",
			func(top *topic.Topic) {
				top.Add("v1")
				top.Add("v2")
			},
			1,
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New()
			tt.f(top)

			_, got, err := top.Get(context.Background(), tt.getID)

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
		name   string
		f      func(*topic.Topic) time.Time
		getID  uint64
		expect []string
	}{
		{
			"Prune after two values",
			func(top *topic.Topic) time.Time {
				top.Add("v1")
				top.Add("v2")
				pruneTime := time.Now()
				top.Add("v3")
				top.Add("v4")
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
				top.Add("v1")
				return time.Now()
			},
			0,
			values("v1"),
		},
		{
			"Do not prune last element",
			func(top *topic.Topic) time.Time {
				top.Add("v1")
				top.Add("v2")
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

			_, got, err := top.Get(context.Background(), 0)
			if err != nil {
				t.Errorf("Get() returned an unexpected error %v", err)
			}
			if !cmpSlice(got, tt.expect) {
				t.Errorf("Got %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestErrUnknownID(t *testing.T) {
	top := topic.New()
	top.Add("v1")
	top.Add("v2")
	ti := time.Now()
	top.Add("v3")
	top.Add("v4")

	top.Prune(ti)

	_, _, err := top.Get(context.Background(), 1)
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
	expect := "id 1 is unknown in topic. Lowest id is 3"
	if got := topicErr.Error(); got != expect {
		t.Errorf("Got error message \"%s\", want \"%s\"", got, expect)
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
				top.Add("v1")
				top.Add("v2")
				top.Add("v3")
			},
			3,
		},
		{
			"Empty Topic",
			func(top *topic.Topic) {},
			0,
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

func TestGetBlocking(t *testing.T) {
	// Tests, that Get() blocks until there is new data.
	top := topic.New()

	// Add a value after a short time.
	go func() {
		time.Sleep(time.Millisecond)
		top.Add("value")
	}()

	// Send values as soon as Get() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Get(context.Background(), 0)
		if err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Get() should return before the timer is over.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-received:
		if !cmpSlice(got, values("value")) {
			t.Errorf("Get() returned %v, expected [value]", got)
		}
	case <-timer.C:
		t.Errorf("Get() blocked fore more then 100 Milliseconds, expected to get data.")
	}
}

func TestBlockUntilClose(t *testing.T) {
	// Tests, that Get() unblocks, when the topic is closed.
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed(closed))

	// Send values as soon as Get() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Get(context.Background(), 0)
		if err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Get() should not return before the timer.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-received:
		t.Errorf("Get() returned before the topic was closed.")
	case <-timer.C:
		// Close the topic after some time.
		close(closed)
	}

	// Get should return after the topic was closed.
	timer.Reset(100 * time.Millisecond)
	select {
	case got := <-received:
		if !cmpSlice(got, values()) {
			t.Errorf("Get() returned %v, expected []", got)
		}
	case <-timer.C:
		t.Errorf("Get() blocked for emore then 100 Milliseconds, expected to unblock after topic is closed.")
	}
}

func TestBlockUntilContexDone(t *testing.T) {
	// Tests, that Get() unblocks, when the context is canceled
	top := topic.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Send values as soon as Get() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Get(ctx, 0)
		if err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Get() should not return before the timer.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-received:
		t.Errorf("Get() returned before the context was canceled.")
	case <-timer.C:
		// Cancel the context after some time.
		cancel()
	}

	// Get should return after context was canceled.
	timer.Reset(100 * time.Millisecond)
	select {
	case got := <-received:
		if !cmpSlice(got, values()) {
			t.Errorf("Get() returned %v, expected []", got)
		}
	case <-timer.C:
		t.Errorf("Get() blocked for emore then 100 Milliseconds, expected to unblock after the context was canceled.")
	}
}

func TestBlockOnHighestID(t *testing.T) {
	// Test, that Get() blocks on a non empty topic when the highest id is requested.
	top := topic.New()
	top.Add("v1")
	top.Add("v2")
	highestID := top.Add("v3")

	// Close done channel, after Get() unblocks.
	done := make(chan struct{})
	go func() {
		if _, _, err := top.Get(context.Background(), highestID); err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		close(done)
	}()

	// Get should not return before the timer
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
		t.Errorf("Get() returned before the done-channel was closed.")
	case <-timer.C:
		// Add another value, this should unblock the topic.
		top.Add("v4")
	}

	// Get should return after context was canceled.
	timer.Reset(100 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Errorf("Get() blocked for emore then 100 Milliseconds, expected to unblock after another value was added.")
	}
}

func TestGetOnClosedTopic(t *testing.T) {
	// Test, that a Get()-call on a already closed topic returnes immediately
	closed := make(chan struct{})
	top := topic.New(topic.WithClosed(closed))
	top.Add("v1")
	highestID := top.Add("v2")
	close(closed)

	// When the topic is closed, Get(0) should still return its data.
	_, got, err := top.Get(context.Background(), 0)
	if err != nil {
		t.Errorf("Get() returned the unexpected error %v", err)
	}
	if !cmpSlice(got, values("v1", "v2")) {
		t.Errorf("Get() returned %v, expected [v1 v2]", got)
	}

	// Send values as soon as Get() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Get(context.Background(), highestID+100)
		if err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Get() should return immediately.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-received:
		if got != nil {
			t.Errorf("Get() returned %v, expected nil", got)
		}
	case <-timer.C:
		t.Errorf("Get() blocked. Expect it to return immediately when the topic is closed.")
	}
}

func TestGetOnCanceledChannel(t *testing.T) {
	top := topic.New()
	top.Add("v1")
	highestID := top.Add("v2")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// When context is canceled, Get(0) should still return its data.
	_, got, err := top.Get(ctx, 0)
	if err != nil {
		t.Errorf("Get() returned the unexpected error %v", err)
	}
	if !cmpSlice(got, values("v1", "v2")) {
		t.Errorf("Get() returned %v, expected [v1 v2]", got)
	}

	// Send values as soon as Get() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.Get(ctx, highestID+100)
		if err != nil {
			t.Errorf("Get() returned the unexpected error %v", err)
		}
		received <- got
	}()

	// Get() should return immediately.
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-received:
		if got != nil {
			t.Errorf("Get() returned %v, expected nil", got)
		}
	case <-timer.C:
		t.Errorf("Get() blocked. Expect it to return immediately when the topic is closed.")
	}
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
