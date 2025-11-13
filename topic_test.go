package topic_test

import (
	"context"
	"errors"
	"sort"
	"sync"
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

			_, got, err := top.ReceiveSince(t.Context(), tt.receiveID)

			if err != nil {
				t.Errorf("Did not expect an error, got: %v", err)
			}
			if !cmpSlice(got, tt.expect) {
				t.Errorf("Got %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestPublishCreatesIncreasingIDs(t *testing.T) {
	top := topic.New[string]()

	id1 := top.Publish("v1")
	id2 := top.Publish("v2", "v3")
	id3 := top.Publish("v4")

	if !(id1 < id2 && id2 < id3) {
		t.Errorf("Got ids %d %d %d, expected increasing", id1, id2, id3)
	}
}

func TestPublishWithoutValues(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")

	id := top.Publish()

	if id != 1 {
		t.Errorf("Publish() without values returned %d, expected 1", id)
	}

	_, data, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if len(data) != 1 {
		t.Errorf("Expected 1 value, got %d", len(data))
	}
}

func TestReceiveWithIDEqualLastID(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	lastID := top.Publish("v2")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, data, err := top.ReceiveSince(ctx, lastID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context deadline exceeded, got %v", err)
	}
	if data != nil {
		t.Errorf("Expected nil data, got %v", data)
	}
}

func TestPrune(t *testing.T) {
	for _, tt := range []struct {
		name   string
		f      func(*topic.Topic[string]) time.Time
		expect []string
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
			values("v3", "v4"),
		},
		{
			"Prune on topic with one element",
			func(top *topic.Topic[string]) time.Time {
				top.Publish("v1")
				return time.Now()
			},
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
			values("v2"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			top := topic.New[string]()
			pruneTime := tt.f(top)

			top.Prune(pruneTime)

			_, got := top.ReceiveAll()

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
	_, got, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Fatalf("Receive(): %v", err)
	}

	if !cmpSlice(got, []string{"foo"}) {
		t.Errorf("Got %v, expect [foo]", got)
	}
}

func TestPruneAllElements(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")

	top.Prune(time.Now())

	if lastID := top.LastID(); lastID != 2 {
		t.Errorf("LastID() = %d, expected 2", lastID)
	}

	ctxCanceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, data, _ := top.ReceiveSince(ctxCanceled, 0)
	if data != nil {
		t.Errorf("Expected nil data after pruning all, got %v", data)
	}
}

func TestPruneUsedValue(t *testing.T) {
	top := topic.New[string]()
	top.Publish("val1")
	top.Publish("val2")
	top.Publish("val3")
	ti := time.Now()
	_, data, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Fatalf("Receive(): %v", err)
	}
	top.Publish("val4")
	top.Prune(ti)

	if data[0] != "val1" {
		t.Errorf("Received value changed to %s, expected `val1`", data[0])
	}
}

func TestPruneWithPastTime(t *testing.T) {
	top := topic.New[string]()

	top.Publish("v1")
	top.Publish("v2")

	pastTime := time.Now().Add(-1 * time.Hour)
	top.Prune(pastTime)

	_, data, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if len(data) != 2 {
		t.Errorf("Expected 2 values after pruning past time, got %d", len(data))
	}
}

func TestMultiplePrunes(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")
	t1 := time.Now()
	top.Publish("v3")
	t2 := time.Now()
	top.Publish("v4")

	top.Prune(t1)
	top.Prune(t2)

	_, data := top.ReceiveAll()
	if len(data) != 1 || data[0] != "v4" {
		t.Errorf("After multiple prunes got %v, expected [v4]", data)
	}
}

func TestReceiveWithExactOffsetAfterPrune(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")
	ti := time.Now()
	top.Publish("v3")

	top.Prune(ti)

	_, data, err := top.ReceiveSince(t.Context(), 2)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if len(data) != 1 || data[0] != "v3" {
		t.Errorf("Receive(2) = %v, expected [v3]", data)
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

	_, _, err := top.ReceiveSince(t.Context(), 1)
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

func TestReceiveWithFutureID(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")

	done := make(chan struct{})
	go func() {
		_, _, err := top.ReceiveSince(t.Context(), 100)
		if err != nil {
			t.Errorf("Receive() returned unexpected error: %v", err)
		}
		close(done)
	}()

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
		t.Error("Receive() should block when ID > lastID")
	case <-timer.C:
		top.Publish("v2")
	}

	timer.Reset(100 * time.Millisecond)
	select {
	case <-done:
	case <-timer.C:
		t.Error("Receive() should unblock after Publish()")
	}
}

func TestReceiveReturnsCorrectID(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	top.Publish("v2")

	id, _, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if id != 2 {
		t.Errorf("Receive() returned id %d, expected 2", id)
	}

	id, _, err = top.ReceiveSince(t.Context(), 1)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if id != 2 {
		t.Errorf("Receive() returned id %d, expected 2", id)
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

func TestLastIDAfterPrune(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1", "v2")
	ti := time.Now()
	top.Publish("v3")

	top.Prune(ti)

	if id := top.LastID(); id != 3 {
		t.Errorf("LastID() after prune = %d, expected 3", id)
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
		_, got, err := top.ReceiveSince(t.Context(), 0)
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
		_, got, err := top.ReceiveSince(ctx, 0)
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
		if _, _, err := top.ReceiveSince(t.Context(), highestID); err != nil {
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

func TestPruneDuringBlockedReceive(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")

	done := make(chan struct{})
	go func() {
		top.ReceiveSince(t.Context(), 1) // Blocking
		close(done)
	}()

	time.Sleep(time.Millisecond)

	// Prune while blocking
	top.Prune(time.Now())

	top.Publish("v2")

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Error("Receive() should unblock after Publish()")
	}
}

func TestReceiveOnCanceledChannel(t *testing.T) {
	top := topic.New[string]()
	top.Publish("v1")
	highestID := top.Publish("v2")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// When context is canceled, Receive(0) should still return its data.
	_, got, err := top.ReceiveSince(ctx, 0)
	if err != nil {
		t.Errorf("Receive() returned the unexpected error %v", err)
	}
	if !cmpSlice(got, values("v1", "v2")) {
		t.Errorf("Receive() returned %v, expected [v1 v2]", got)
	}

	// Send values as soon as Receive() returnes.
	received := make(chan []string)
	go func() {
		_, got, err := top.ReceiveSince(ctx, highestID+100)
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

func TestConcurrentPublishes(t *testing.T) {
	top := topic.New[int]()

	const numPublishers = 10
	const publishesPerPublisher = 100

	var wg sync.WaitGroup
	for i := range numPublishers {
		wg.Go(func() {
			for j := range publishesPerPublisher {
				top.Publish(i*publishesPerPublisher + j)
			}
		})
	}

	wg.Wait()

	_, data, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}

	expectedLen := numPublishers * publishesPerPublisher
	if len(data) != expectedLen {
		t.Errorf("Expected %d values, got %d", expectedLen, len(data))
	}
}

func TestMultipleConcurrentReceives(t *testing.T) {
	top := topic.New[string]()

	const numReceivers = 100
	var wg sync.WaitGroup

	results := make([][]string, numReceivers)
	for i := range numReceivers {
		wg.Go(func() {
			_, data, err := top.ReceiveSince(t.Context(), 0)
			if err != nil {
				t.Errorf("Receive() error: %v", err)
				return
			}
			results[i] = data
		})
	}

	time.Sleep(time.Millisecond)
	top.Publish("value")

	wg.Wait()

	for i, result := range results {
		if len(result) != 1 || result[0] != "value" {
			t.Errorf("Receiver %d got %v, expected [value]", i, result)
		}
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

	_, values, err := top.ReceiveSince(t.Context(), 0)
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

	_, values, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("receive: %v", err)
	}

	expect := myType{5, "foobar"}
	if len(values) != 2 || *values[0] != expect || *values[1] != expect {
		t.Errorf("got %v, expected [%v %v]", values, expect, expect)
	}
}

func TestTopicWithNilPointers(t *testing.T) {
	top := topic.New[*string]()
	top.Publish(nil, nil)

	_, data, err := top.ReceiveSince(t.Context(), 0)
	if err != nil {
		t.Errorf("Receive() error: %v", err)
	}
	if len(data) != 2 || data[0] != nil || data[1] != nil {
		t.Errorf("Expected [nil, nil], got %v", data)
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
