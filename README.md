# Topic

[![Actions Status](https://github.com/ostcar/topic/workflows/Topic/badge.svg)](https://github.com/ostcar/topic/actions)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/ostcar/topic)](https://pkg.go.dev/github.com/ostcar/topic)

Package topic is an in process pubsub system where new values have to be pulled
instead of beeing pushed.

The idea of pulling updates is inspired by [Kafka](https://kafka.apache.org/) or
[Redis-Streams](https://redis.io/topics/streams-intro). A subscriber does not
have to register or unsubscribe to a topic and can take as long as it needs to
process the messages.


## How to use topic

A topic can be created with: `top := topic.New[string]()`.

To publish one or more values, use: `top.Publish("info1", "info2")`.

To receive values for the first time use: `id, values, err := top.Receive(ctx,
0)`. The first value is a numeric id, it is needed for for next call of
`top.Receive()`. The second argument is a list of all values that where
published by this topic.

To receive newer values, use `id, values, err = top.Receive(ctx, id)`. It
returns all values that published after the given `id`.

A topic is save for concurrent use.


## Run tests

Contibutions are wellcome. Please make sure that the tests are running with:

```go test```
