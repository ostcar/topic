package topic

// Option is an optional argument for the topic.New() constructor.
type Option func(*Topic)

// WithClosed adds a close-channel to the topic. When the given channel is
// closed, all waiting Receive()-calls get unblocked.
func WithClosed(closed <-chan struct{}) Option {
	return func(top *Topic) {
		top.closed = closed
	}
}

// WithStartID sets the lowest id of the topic to a given id.
func WithStartID(id uint64) Option {
	return func(top *Topic) {
		top.Publish()
		top.head.id = id
		delete(top.index, 1)
		top.index[id] = top.head
	}
}
