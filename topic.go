package topic

import (
	"context"
	"slices"
	"sync"
	"time"
)

// Topic is a datastructure that holds a list of values. To add a value to a
// topic is called publishing that value. Each time, a list of values is
// published, a new id is created. It is possible to receive all values from a
// topic at once or the values that were published after a specific id.
//
// A Topic has to be created with `topic.New()`. For example
// topic.New[string]().
//
// A Topic is safe for concurrent use.
type Topic[T any] struct {
	mu sync.RWMutex

	data       []T
	insertTime []time.Time
	offset     uint64

	// The signal channel is closed when data is published by the topic to
	// signal all listening Receive()-calls. After closing the channel, a new
	// channel is created and saved into this variable, so other Receive()-calls
	// can listen on it.
	signal chan struct{}
}

// New creates a new topic.
func New[T any]() *Topic[T] {
	top := &Topic[T]{
		signal: make(chan struct{}),
	}

	return top
}

// Publish adds one or many values to a topic. It returns the new id. All
// waiting Receive()-calls are awakened.
func (t *Topic[T]) Publish(value ...T) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.data = append(t.data, value...)

	// time.Now() uses a syscall and therefore is slow (around 3500 ns).
	now := time.Now()
	t.insertTime = slices.Grow(t.insertTime, len(value))
	for range len(value) {
		t.insertTime = append(t.insertTime, now)
	}

	// Closes the signal channel to signal all Receive()-calls. To overwrite the
	// value afterwards is not a race condition. Since the go-implementation of a
	// channel is a pointer-type, a new object is created, while the
	// Receive()-calls keep listening on the old object.
	close(t.signal)
	t.signal = make(chan struct{})

	return t.lastID()
}

// Receive returns all values from the topic. If id is 0, all values are
// returned. Otherwise, all values that were inserted after the id are returned.
//
// If the id is lower than the lowest id in the topic, an error of type
// UnknownIDError is returned.
//
// If there is no new data, Receive() blocks until there is new data or the
// given channel is done. The same happens with id 0, when there is no data at
// all in the topic.
//
// For performance reasons, this function returns the internal slice of the
// topic. It is not allowed to manipulate the values.
func (t *Topic[T]) Receive(ctx context.Context, id uint64) (uint64, []T, error) {
	t.mu.RLock()
	lastIDWhenStarted := t.lastID()

	// Request data, that is not in the topic yet. Block until the next
	// Publish() call.
	if t.data == nil || id >= lastIDWhenStarted {
		c := t.signal
		t.mu.RUnlock()

		select {
		case <-c:
			return t.Receive(ctx, lastIDWhenStarted)
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}

	defer t.mu.RUnlock()

	if id == 0 {
		return t.lastID(), t.data, nil
	}

	if id < t.offset {
		return 0, nil, UnknownIDError{ID: id, FirstID: t.offset + 1}
	}
	return t.lastID(), t.data[id-t.offset:], nil
}

// LastID returns the last id of the topic. Returns 0 for an empty topic.
func (t *Topic[T]) LastID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.lastID()
}

func (t *Topic[T]) lastID() uint64 {
	return uint64(len(t.data)) + t.offset
}

// Prune removes entries from the topic that are older than the given time.
func (t *Topic[T]) Prune(until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.data) == 0 {
		return
	}

	n, _ := slices.BinarySearchFunc(t.insertTime, until, time.Time.Compare)

	if n == 0 {
		return
	}

	if n >= len(t.data) {
		t.data = nil
		t.insertTime = nil
		t.offset += uint64(n)
		return
	}

	t.data = slices.Clone(t.data[n:])
	t.insertTime = slices.Clone(t.insertTime[n:])
	t.offset += uint64(n)
}
