package topic_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/ostcar/topic"
)

func benchmarkPublishWithXReceivers(count int, b *testing.B) {
	closed := make(chan struct{})
	defer close(closed)
	top := topic.New(topic.WithClosed[string](closed))
	for i := 0; i < count; i++ {
		// Starts a receiver that listens to the topic until the topic is closed.
		go func() {
			var id uint64
			var values []string
			for {
				id, values, _ = top.Receive(context.Background(), id)
				if len(values) == 0 {
					return
				}
			}
		}()
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Publish("value")
	}
}

func BenchmarkPublishWithXReceivers1(b *testing.B)     { benchmarkPublishWithXReceivers(1, b) }
func BenchmarkPublishWithXReceivers10(b *testing.B)    { benchmarkPublishWithXReceivers(10, b) }
func BenchmarkPublishWithXReceivers100(b *testing.B)   { benchmarkPublishWithXReceivers(100, b) }
func BenchmarkPublishWithXReceivers1000(b *testing.B)  { benchmarkPublishWithXReceivers(1_000, b) }
func BenchmarkPublishWithXReceivers10000(b *testing.B) { benchmarkPublishWithXReceivers(10_000, b) }

func benchmarkRetrieveBigTopic(count int, b *testing.B) {
	top := topic.New[string]()
	for i := 0; i < count; i++ {
		top.Publish("value" + strconv.Itoa(i))
	}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Receive(context.Background(), 0)
	}
}

func BenchmarkRetrieveBigTopic1(b *testing.B)      { benchmarkRetrieveBigTopic(1, b) }
func BenchmarkRetrieveBigTopic10(b *testing.B)     { benchmarkRetrieveBigTopic(10, b) }
func BenchmarkRetrieveBigTopic100(b *testing.B)    { benchmarkRetrieveBigTopic(100, b) }
func BenchmarkRetrieveBigTopic1000(b *testing.B)   { benchmarkRetrieveBigTopic(1_000, b) }
func BenchmarkRetrieveBigTopic10000(b *testing.B)  { benchmarkRetrieveBigTopic(10_000, b) }
func BenchmarkRetrieveBigTopic100000(b *testing.B) { benchmarkRetrieveBigTopic(100_000, b) }

func benchmarkRetrieveLastBigTopic(count int, b *testing.B) {
	top := topic.New[string]()
	for i := 0; i < count; i++ {
		top.Publish("value" + strconv.Itoa(i))
	}
	id := top.LastID()
	ctx := context.Background()

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		top.Receive(ctx, id-1)
	}
}

func BenchmarkRetrieveLastBigTopic1(b *testing.B)      { benchmarkRetrieveLastBigTopic(1, b) }
func BenchmarkRetrieveLastBigTopic10(b *testing.B)     { benchmarkRetrieveLastBigTopic(10, b) }
func BenchmarkRetrieveLastBigTopic100(b *testing.B)    { benchmarkRetrieveLastBigTopic(100, b) }
func BenchmarkRetrieveLastBigTopic1000(b *testing.B)   { benchmarkRetrieveLastBigTopic(1_000, b) }
func BenchmarkRetrieveLastBigTopic10000(b *testing.B)  { benchmarkRetrieveLastBigTopic(10_000, b) }
func BenchmarkRetrieveLastBigTopic100000(b *testing.B) { benchmarkRetrieveLastBigTopic(100_000, b) }
