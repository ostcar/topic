/*
Package topic is an in-memory pubsub system where new values are pulled instead
of being pushed.

It solves the problem, that you want to publish data to many goroutines. The
standard way in go uses channels to push values to the readers. But channels
have the problems, that either the goroutine sending the data has to wait for
the reader or has to discard messages, if the reader is too slow. A buffered
channel can help to delay the problem, but eventually the buffer could be full.

The idea of pulling updates is inspired by Kafka or Redis-Streams. A subscriber
does not have to register or unsubscribe to a topic and can take as much time as
it needs to process the messages. Therefore, the system is less error prone.

In a pulling messaging system, the publisher does not push the values, but the
receivers have to pull them. The publisher can save values without waiting on
slow receivers. A receiver has all the time it needs to process messages and can
pull again as soon as the work is done.

Another benefit of this pattern is, that a receiver does not have to register on
the pubsub system. Since the publisher does not send the messages, it does not
have to know how many receivers there are. Therefore there are no register or
unregister methods in this package.

# Create new topic

To create a new topic use the topic.New() constructor:

	top := topic.New[string]()

# Publish messages

Messages can be published with the Publish()-method:

	top.Publish("some value")

More than one message can be published at once:

	top.Publish("some value", "other value")

Internally, the topic creates a new id that can be used to receive newer values.
The Publish()-method returns this id. In most cases, the returned id can be
ignored.

# Receive messages

Messages can be received with the ReceiveAll()- or ReceiveSince()-method:

	id, values := topic.ReceiveAll()
	id, values, err := top.ReceiveSince(context.Background(), 42)

The returned id is the number of values in the topic. It can only increase.

The returned values are a slice of the published messages.

To receive newer values, ReceiveSince() can be called again with the id from the last
call:

	id, values, err := top.ReceiveAll()
	...
	id, values, err = top.ReceiveSince(context.Background(), id)

Only messages, that were published after the given id are returned.

When there are no new values in the topic, then the ReceiveSince()-call blocks until
there are new values. To add a timeout to the call, the context can be used:

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, values, err = top.ReceiveSince(ctx, id)

If there are no new values before the context is canceled, the topic returns the error
of the context. For example `context.DeadlineExceeded` or `context.Canceled`.

The usual pattern to subscribe to a topic is:

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var id uint64
	var values []string
	var err error
	for {
	    id, values, err = top.ReceiveSince(ctx, id)
	    if err != nil {
	        if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
	            // Timeout
	            break
	        }
	        // Handle other errors
	    }
	    // Process values
	}

The loop will process all values published to the topic for one minute.

# Get Last ID

The example above will process all messages in the topic. If only messages
should be processed that were published after the loop starts, the method
LastID() can be used:

	id := top.LastID()
	id, values, err = top.ReceiveSince(context.Background(), id)

The return value of LastID() is the highest id in the topic. So
a ReceiveSince() call on top.LastID() will only return data that
was published after the call.

A pattern to receive only new data is:

	id := top.LastID()
	var values []string
	var err error
	for {
	    id, values, err = top.ReceiveSince(context.Background(), id)
	    if err != nil {
	        // Handle error
	    }
	    // Process values
	}

# Prune old values

For this pattern to work, the topic has to save all values that were ever
published. To free some memory, old values can be deleted from time to time.
This can be accomplished with the Prune() method:

	top.Prune(time.Now().Add(-10*time.Minute))

This call will remove all values in the topic that are older than ten minutes.

Make sure that all receivers have read the values before they are pruned.

If a Receive()-call tries to receive pruned values, it will return with the
error `topic.UnknownIDError`.
*/
package topic
