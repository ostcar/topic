package topic

import (
	"context"
)

// Select listens on many topics at the same time. It returns the index of the
// topic in the given slice, that where processed and the return values from
// receive method of this topic.
//
// At the end, the TID of the Case is updated, so the same slice of cases can be
// called again.
//
// topic.Select() behaves simular to the golang select call. topic.Select() with
// an empty slice of cases will block forever. If a Case contails a nil-Topic,
// it will never be used. A topic that is closed, will never block.
func Select(ctx context.Context, cases ...*Case) (index int, tid uint64, values []string, err error) {
	rc := make(chan result)
	done := make(chan struct{})

	for idx, c := range cases {
		if c.Topic == nil {
			continue
		}

		cancelCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func(ctx context.Context, idx int, c *Case) {
			id, v, err := c.Topic.Receive(ctx, c.ID)

			select {
			case rc <- result{idx, id, v, err}:
			case <-done:
			}
		}(cancelCtx, idx, c)
	}

	r := <-rc
	close(done)
	cases[r.idx].ID = r.id
	return r.idx, r.id, r.values, r.err
}

// Case is used in the topic.Select() function to specify a topic and a topic id.
type Case struct {
	ID    uint64
	Topic *Topic
}

type result struct {
	idx    int
	id     uint64
	values []string
	err    error
}
