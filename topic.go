package topic

import (
	"context"
	"sync"
	"time"
)

// Topic is a datastructure that holds a set of strings. Each time a list of
// strings are published by the topic, a new id is created. It is possible to
// retrive all strings at once or the strings that published after a specivic
// id.
//
// A Topic has to be created with the topic.New() function.
//
// A Topic is save for concourent use.
type Topic struct {
	mu     sync.RWMutex
	closed <-chan struct{}

	// The topic is implemented by a linked list and an index from each id to
	// the node. Therefore nodes get be added, retrieved and deleted from the
	// top in constant time.
	head  *node
	tail  *node
	index map[uint64]*node

	// The signal channel is closed when data is published by the topic to
	// signal all listening Retrive()-calls. After closing the channel, a new
	// channel is created and saved into this variable, so other Retrive()-calls
	// can listen on it.
	signal chan struct{}
}

// New creates a new topic. The topic can be initialized with a close channel
// with the topic.WithClosed() option.
func New(options ...Option) *Topic {
	top := &Topic{
		signal: make(chan struct{}),
		index:  make(map[uint64]*node),
	}

	for _, o := range options {
		o(top)
	}
	return top
}

// Publish adds a list of strings to a topic. It creates a new id and returns
// it. All waiting Retrive()-calls are awakened.
//
// Publish() inserts the values in constant time.
func (t *Topic) Publish(value ...string) uint64 {
	if len(value) == 0 {
		return t.LastID()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	newNode := &node{}
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

	// Closes the signal channel to signal all Retrive()-calls. To overwrite the
	// value afterwars is not a race condition. Since the go-implementation of a
	// channel is a pointer-type, a new object is created, while the
	// Retrive()-calls keep listening on the old object.
	close(t.signal)
	t.signal = make(chan struct{})

	return newNode.id
}

// Receive returns a slice of unique strings from the topic. If id is 0, all
// strings are returned, else, all strings that where inserted after the id are
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
// always unblocking. On existing ids it returns the values as before. On the
// ids equal or higher to the highest id, it returns nil.
//
// If the data is available, Receive() returns in O(n) where n is the number of
// values in the topic since the given id.
func (t *Topic) Receive(ctx context.Context, id uint64) (uint64, []string, error) {
	t.mu.RLock()

	// Request data, that is not in the topic yet. Block until the next
	// Publish() call.
	if t.tail == nil || id >= t.tail.id {
		c := t.signal
		t.mu.RUnlock()

		select {
		case <-c:
			return t.Receive(ctx, id)
		case <-t.closed:
		case <-ctx.Done():
		}

		// In very rare cases, new data could be added to the topic at the same
		// time as the topic was closed. If that happens, it is pseudo random,
		// which select-case the golang runtime chooses. To always return all
		// data, we have to check for new data here.
		if t.LastID() > id {
			return t.Receive(ctx, id)
		}

		// The topic or the condext is closed. Return without data.
		return id, nil, nil
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

// LastID returns the last if of topic. Returns 0 for an empty topic.
//
// LastID returns in constant time.
func (t *Topic) LastID() uint64 {
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
func (t *Topic) Prune(until time.Time) {
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
type node struct {
	id    uint64
	t     time.Time
	next  *node
	value []string
}

// runNode returns all strings from a node and the following nodes. Each value is unique.
func runNode(n *node) []string {
	var values []string
	seen := make(map[string]bool)
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
