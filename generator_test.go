package meb_test

import (
	"testing"
	"time"

	"github.com/supershabam/meb"
)

func TestGenerate(t *testing.T) {
	c := meb.GenConfig{
		Key:    "hi",
		Period: time.Millisecond,
		Value:  func() float64 { return 73.2 },
	}
	done := make(chan struct{})
	events := meb.Generate(done, c)
	event, ok := <-events
	if !ok {
		t.Fail()
	}
	if event.Value != 73.2 {
		t.Fail()
	}
	close(done)
	event, ok = <-events
	if ok {
		t.Fail()
	}
}
