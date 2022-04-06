package topic

import (
	"context"
	"sync"
	"time"
)

// Topic is a datastructure that holds a set values. Values can be published to
// a topic. Each time a list of values is published, a new id is created. It is
// possible to receive all values at once or the values that published after a
// specivic id.
//
// A Topic has to be created with the topic.New() function. For example
// topic.New[string]().
//
// A Topic is save for concourent use.
//
// The type of value is restricted to be a comparable. This is required, so the
// topic.Receive function can return a list of unique values. This restriction
// could be removed in a future version of go, if it will be possible to check
// the type of a generic value.
type Topic[T comparable] struct {
	mu     sync.RWMutex
	closed <-chan struct{}

	// The topic is implemented by a linked list and an index from each id to
	// the node. Therefore nodes get be added, retrieved and deleted from the
	// top in constant time.
	head  *node[T]
	tail  *node[T]
	index map[uint64]*node[T]

	// The signal channel is closed when data is published by the topic to
	// signal all listening Receive()-calls. After closing the channel, a new
	// channel is created and saved into this variable, so other Receive()-calls
	// can listen on it.
	signal chan struct{}
}

// New creates a new topic.
//
// The topic can be initialized with a close channel with the topic.WithClosed()
// option.
func New[T comparable](options ...Option[T]) *Topic[T] {
	top := &Topic[T]{
		signal: make(chan struct{}),
		index:  make(map[uint64]*node[T]),
	}

	for _, o := range options {
		o(top)
	}
	return top
}

// Publish adds a list of values to a topic. It creates a new id and returns
// it. All waiting Receive()-calls are awakened.
//
// Publish() inserts the values in constant time.
func (t *Topic[T]) Publish(value ...T) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	newNode := &node[T]{}
	var id uint64
	if t.head == nil {
		t.head = newNode
	} else {
		id = t.tail.id
		t.tail.next = newNode
	}
	t.tail = newNode
	newNode.id = id + 1
	newNode.t = time.Now()
	newNode.value = value

	t.index[newNode.id] = newNode

	// Closes the signal channel to signal all Receive()-calls. To overwrite the
	// value afterwars is not a race condition. Since the go-implementation of a
	// channel is a pointer-type, a new object is created, while the
	// Receive()-calls keep listening on the old object.
	close(t.signal)
	t.signal = make(chan struct{})

	return newNode.id
}

// Receive returns a slice of unique values from the topic. If id is 0, all
// values are returned, else, all values that where inserted after the id are
// returned.
//
// If the id is lower then the lowest id in the topic, an error of type
// ErrUnknownTopicID is returned.
//
// If there is no new data, Receive() blocks until threre is new data or the
// topic is closed or the given context is canceled. The same happens with id 0,
// when there is no data at all in the topic.
//
// If the topic is already closed or the context is canceled, Receive() is
// always unblocking. On exiting it returns the values as before. When the id
// equal or higher to the highest id, it returns an error with the method
// Closing().
//
// If the data is available, Receive() returns in O(n) where n is the number of
// values in the topic since the given id.
func (t *Topic[T]) Receive(ctx context.Context, id uint64) (uint64, []T, error) {
	t.mu.RLock()

	// Request data, that is not in the topic yet. Block until the next
	// Publish() call.
	if t.tail == nil || id >= t.tail.id {
		c := t.signal
		t.mu.RUnlock()

		var err error
		select {
		case <-c:
			return t.Receive(ctx, id)
		case <-t.closed:
			err = closingError{}
		case <-ctx.Done():
			err = ctx.Err()
		}

		// In very rare cases, new data could be added to the topic at the same
		// time as the topic was closed. If that happens, it is pseudo random,
		// which select-case the golang runtime chooses. To always return all
		// data, we have to check for new data here.
		if t.LastID() > id {
			return t.Receive(ctx, id)
		}

		// The topic or the condext is closed.
		return id, nil, err
	}

	defer t.mu.RUnlock()

	if id == 0 {
		// Return all data.
		return t.tail.id, runNode(t.head), nil
	}

	n := t.index[id]
	if n == nil {
		return 0, nil, UnknownIDError{ID: id, FirstID: t.head.id}
	}
	return t.tail.id, runNode(n.next), nil
}

// LastID returns the last id of the topic. Returns 0 for an empty topic.
//
// LastID returns in constant time.
func (t *Topic[T]) LastID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.tail == nil {
		return 0
	}
	return t.tail.id
}

// Prune removes entries from the topic that are older then the given time.
//
// Prune has a complexity of O(n) where n is the count of all nodes that are
// older then the given time.
func (t *Topic[T]) Prune(until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.head == nil {
		return
	}

	// Delete all nodes from the index, that are older then the given time.
	// After the loop, n is the oldes index, that is still in the index.
	n := t.head
	for ; n.t.Before(until) && n.next != nil; n = n.next {
		delete(t.index, n.id)
	}
	t.head = n
}

// node implements a linked list.
type node[T comparable] struct {
	id    uint64
	t     time.Time
	next  *node[T]
	value []T
}

// runNode returns all values from a node and the following nodes. Each value is
// unique. If there are no values, an empty slice (not nil) is returned.
func runNode[T comparable](n *node[T]) []T {
	var values []T
	seen := make(map[T]bool)
	for ; n != nil; n = n.next {
		for _, v := range n.value {
			if !seen[v] {
				values = append(values, v)
				seen[v] = true
			}
		}
	}
	return values
}
