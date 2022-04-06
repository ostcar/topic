package topic

// Option is an optional argument for the topic.New() constructor.
type Option[T comparable] func(*Topic[T])

// WithClosed adds a close-channel to the topic. When the given channel is
// closed, all waiting Receive()-calls get unblocked.
func WithClosed[T comparable](closed <-chan struct{}) Option[T] {
	return func(top *Topic[T]) {
		top.closed = closed
	}
}

// WithStartID sets the lowest id of the topic to a given id.
func WithStartID[T comparable](id uint64) Option[T] {
	return func(top *Topic[T]) {
		top.Publish()
		top.head.id = id
		delete(top.index, 1)
		top.index[id] = top.head
	}
}
