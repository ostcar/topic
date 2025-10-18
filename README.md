# Topic

[![Actions Status](https://github.com/ostcar/topic/workflows/Topic/badge.svg)](https://github.com/ostcar/topic/actions)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/ostcar/topic)](https://pkg.go.dev/github.com/ostcar/topic)

Package topic is an in-process pubsub system where new values can be pulled
instead of being pushed.

The idea of pulling updates is inspired by [Kafka](https://kafka.apache.org/) or
[Redis-Streams](https://redis.io/topics/streams-intro). A subscriber does not
have to register or unsubscribe to a topic and can take as long as it needs to
process the messages.


## How to use topic

A topic can be created with: `top := topic.New[string]()`.

To publish one or more values, use: `top.Publish("info1", "info2")`.

To receive values for the first time use: `id, values, err := top.Receive(ctx,
0)`. The first value is a numeric id, it is needed for the next call of
`top.Receive()`. The second argument is a list of all values that were
published by this topic.

To receive newer values, use `id, values, err = top.Receive(ctx, id)`. It
returns all values that were published after the given `id`.

A topic is safe for concurrent use.

## Run tests

Contributions are welcome. Please make sure that the tests are running with:

```go test```

## Who is using topic

Topic was built for [OpenSlides](https://openslides.com) and is used in
production for the
[Autoupdate-Service](https://github.com/openslides/openslides-autoupdate-service)
for many years without any problems.
