package topic_test

import (
	"context"
	"testing"
	"time"

	"github.com/ostcar/topic"
)

func TestSelect(t *testing.T) {
	t1 := topic.New()
	t2 := topic.New()
	t1.Publish("v1")

	idx, tid, values, err := topic.Select(context.Background(), &topic.Case{Topic: t1}, &topic.Case{Topic: t2})

	if err != nil {
		t.Errorf("Select returned unexpected error: %v", err)
	}
	if idx != 0 {
		t.Errorf("Select returned index %d, expected 0", idx)
	}
	if tid != 1 {
		t.Errorf("Select returned tid %d, expected 1", tid)
	}
	if len(values) != 1 || values[0] != "v1" {
		t.Errorf("Select returned values %v, expected [v1]", values)
	}
}

func TestSelectBlocking(t *testing.T) {
	t1 := topic.New()
	t2 := topic.New()

	var idx int
	var tid uint64
	var values []string
	var err error
	selectDone := make(chan struct{})
	go func() {
		idx, tid, values, err = topic.Select(context.Background(), &topic.Case{Topic: t1}, &topic.Case{Topic: t2})
		close(selectDone)
	}()

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-selectDone:
		t.Errorf("Select call did not block")
	case <-timer.C:
	}

	t1.Publish("v1")
	timer.Reset(time.Millisecond)
	select {
	case <-timer.C:
		t.Errorf("Select did not returen after one Millisecond")
	case <-selectDone:
	}

	if err != nil {
		t.Errorf("Select returned unexpected error: %v", err)
	}
	if idx != 0 {
		t.Errorf("Select returned index %d, expected 0", idx)
	}
	if tid != 1 {
		t.Errorf("Select returned tid %d, expected 1", tid)
	}
	if len(values) != 1 || values[0] != "v1" {
		t.Errorf("Select returned values %v, expected [v1]", values)
	}
}

func TestSelectMultiCalls(t *testing.T) {
	t1 := topic.New()
	t2 := topic.New()

	t1.Publish("v1")

	cases := []*topic.Case{
		{Topic: t1},
		{Topic: t2},
	}

	_, _, _, err := topic.Select(context.Background(), cases...)
	if err != nil {
		t.Errorf("Select returned the error: %v", err)
	}

	t2.Publish("v2")

	idx, tid, values, err := topic.Select(context.Background(), cases...)

	if err != nil {
		t.Errorf("Select returned unexpected error: %v", err)
	}
	if idx != 1 {
		t.Errorf("Select returned index %d, expected 1", idx)
	}
	if tid != 1 {
		t.Errorf("Select returned tid %d, expected 1", tid)
	}
	if len(values) != 1 || values[0] != "v2" {
		t.Errorf("Select returned values %v, expected [v1]", values)
	}
}

func TestSelectEmptyCases(t *testing.T) {
	selectDone := make(chan struct{})
	go func() {
		topic.Select(context.Background())
		close(selectDone)
	}()

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-selectDone:
		t.Errorf("Select call did not block")
	case <-timer.C:
	}
}

func TestSelectNilTopic(t *testing.T) {
	selectDone := make(chan struct{})
	go func() {
		topic.Select(context.Background(), &topic.Case{})
		close(selectDone)
	}()

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-selectDone:
		t.Errorf("Select call did not block")
	case <-timer.C:
	}
}

func TestSelectClosedTopic(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	top := topic.New(topic.WithClosed(closed))

	selectDone := make(chan struct{})
	go func() {
		topic.Select(context.Background(), &topic.Case{Topic: top, ID: 100})
		close(selectDone)
	}()

	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-selectDone:
	case <-timer.C:
		t.Errorf("Select call did block")
	}
}
