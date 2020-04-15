package topic_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/ostcar/topic"
)

func benchmarkAddWithXReceivers(count int, b *testing.B) {
	closed := make(chan struct{})
	defer close(closed)
	top := topic.New(topic.WithClosed(closed))
	for i := 0; i < count; i++ {
		// Starts a receiver that listens to the topic until the topic is closed.
		go func() {
			var id uint64
			var values []string
			for {
				id, values, _ = top.Get(context.Background(), id)
				if len(values) == 0 {
					return
				}
			}
		}()
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Add("value")
	}
}

func BenchmarkAddWithXReceivers1(b *testing.B)     { benchmarkAddWithXReceivers(1, b) }
func BenchmarkAddWithXReceivers10(b *testing.B)    { benchmarkAddWithXReceivers(10, b) }
func BenchmarkAddWithXReceivers100(b *testing.B)   { benchmarkAddWithXReceivers(100, b) }
func BenchmarkAddWithXReceivers1000(b *testing.B)  { benchmarkAddWithXReceivers(1_000, b) }
func BenchmarkAddWithXReceivers10000(b *testing.B) { benchmarkAddWithXReceivers(10_000, b) }

func benchmarkReadBigTopic(count int, b *testing.B) {
	top := topic.New()
	for i := 0; i < count; i++ {
		top.Add("value" + strconv.Itoa(i))
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Get(context.Background(), 0)
	}
}

func BenchmarkReadBigTopic1(b *testing.B)      { benchmarkReadBigTopic(1, b) }
func BenchmarkReadBigTopic10(b *testing.B)     { benchmarkReadBigTopic(10, b) }
func BenchmarkReadBigTopic100(b *testing.B)    { benchmarkReadBigTopic(100, b) }
func BenchmarkReadBigTopic1000(b *testing.B)   { benchmarkReadBigTopic(1_000, b) }
func BenchmarkReadBigTopic10000(b *testing.B)  { benchmarkReadBigTopic(10_000, b) }
func BenchmarkReadBigTopic100000(b *testing.B) { benchmarkReadBigTopic(100_000, b) }

func benchmarkReadLastBigTopic(count int, b *testing.B) {
	top := topic.New()
	for i := 0; i < count; i++ {
		top.Add("value" + strconv.Itoa(i))
	}
	id := top.LastID()
	ctx := context.Background()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Get(ctx, id-1)
	}
}

func BenchmarkReadLastBigTopic1(b *testing.B)      { benchmarkReadLastBigTopic(1, b) }
func BenchmarkReadLastBigTopic10(b *testing.B)     { benchmarkReadLastBigTopic(10, b) }
func BenchmarkReadLastBigTopic100(b *testing.B)    { benchmarkReadLastBigTopic(100, b) }
func BenchmarkReadLastBigTopic1000(b *testing.B)   { benchmarkReadLastBigTopic(1_000, b) }
func BenchmarkReadLastBigTopic10000(b *testing.B)  { benchmarkReadLastBigTopic(10_000, b) }
func BenchmarkReadLastBigTopic100000(b *testing.B) { benchmarkReadLastBigTopic(100_000, b) }
