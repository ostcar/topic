/*
Package topic is an in-memory pubsub system where new values are pulled instead of
being pushed.

The idea of pulling updates is inspired by Kafka or Redis-Streams. A subscriber
does not have to register or unsubscribe to a topic and can take as much time as
it needs to process the messages. Therefore, the system is less error prone.

In common pubsub systems, the publisher pushes values to the receivers. The
problem with this pattern is, that the publisher could send messages faster
than a slow receiver can process them. A buffer can help to delay the problem,
but eventually the buffer could be full. If this happens, there are two options.
Either the publisher has to wait on the slowest receiver or a slow receiver has
to drop messages. In the first case, the system is only as fast as the slowest
receiver. In the second case, it is not guaranteed that a receiver gets all
messages.

A third pattern is, that the publisher does not push the values, but the
receivers have to pull them. The publisher can save values without waiting on
slow receivers. A receiver has all the time it needs to process messages and can
pull again as soon as the work is done. This package implements the third
pattern.

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

Messages can be received with the Receive()-method:

	id, values, err := top.Receive(context.Background(), 0)

The first returned value is the id created by the last Publish()-call. The
second value is a slice of all messages that were published before. Each
value in the returned slice is unique.

To receive newer values, Receive() can be called again with the id from the last
call:

	id, values, err := top.Receive(context.Background(), 0)
	...
	id, values, err = top.Receive(context.Background(), id)

When the given id is zero, then all messages are returned. If the id is greater
than zero, then only messages are returned that were published by the topic
after the id was created.

When there are no new values in the topic, then the Receive()-call blocks until
there are new values. To add a timeout to the call, the context can be used:

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, values, err = top.Receive(ctx, id)

If there are no new values before the context is canceled, the topic returns
with the error `context.DeadlineExceeded`.

The usual pattern to subscribe to a topic is:

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var id uint64
	var values []string
	var err error
	for {
	    id, values, err = top.Receive(ctx, id)
	    if err != nil {
	        if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
	            // Timeout
	            break
	        }
	        // Handle other errors
	    }
	    // Process values
	}

The loop will process all values published by the topic for one minute.

# Get Last ID

The example above will process all messages in the topic. If only messages
should be processed that were published after the loop starts, the method
LastID() can be used:

	id := top.LastID()
	id, values, err = top.Receive(context.Background(), id)

The return value of LastID() is the highest id in the topic. So a Receive() call
on top.LastID() will only return data that was published after the call.

A pattern to receive only new data is:

	id := top.LastID()
	var values []string
	var err error
	for {
	    id, values, err = top.Receive(context.Background(), id)
	    if err != nil {
	        // Handle error
	    }
	    // Process values
	}

# Prune old values

For this pattern to work, the topic has to save all values that were ever
published. To free some memory, old values can be deleted from time to time.
This can be accomplished with the Prune() method:

	top.Prune(10*time.Minute)

This call will remove all values in the topic that are older than ten minutes.

Make sure that all receivers have read the values before they are pruned.

If a Receive()-call tries to receive pruned values, it will return with the
error `topic.ErrUnknownID`.
*/
package topic
