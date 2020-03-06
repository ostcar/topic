# Topic

[![Actions](https://github.com/ostcar/topic/workflows/CI/badge.svg)](https://github.com/ostcar/topic)
[![GoDoc](https://godoc.org/github.com/ostcar/topic?status.svg)](https://godoc.org/github.com/ostcar/topic)

Package topic is a in process pubsub system where new values have to
be pulled instead of beeing pushed.

The idea of pulling updates is inspired by [Kafka](https://kafka.apache.org/) or
[Redis-Streams](https://redis.io/topics/streams-intro). A subscriber
does not have to register or unsubscribe to a topic and can take as long as it needs to
process the messages.


## How to use topic

A topic can be created with: `top := topic.New()`.

To publish one or more strings, use: `top.Add("info1", "info2")`.

To receive values for the first time use: `tid, values, err := top.Get(ctx, 0)`. The first
value is a numeric id, it is needed for for next call of `top.Get()`. The second argument
is a list of all strings that where published in this topic.

To receive newer values, use `tid, values, err = top.Get(ctx, tid)`. It returns all values that
added after the given `tid`.

A topic is save for concurrent use.


## Run tests

Contibutions are wellcome. Please make sure that the tests are running with:

```go test```