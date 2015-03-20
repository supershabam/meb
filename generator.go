package meb

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type GenConfig struct {
	Key    string
	Period time.Duration
	Value  func() float64
}

func Generate(done <-chan struct{}, c GenConfig) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		select {
		case <-done:
			return
		case <-time.After(time.Duration(rand.Int63n(int64(c.Period)))):
		}
		t := time.NewTicker(c.Period)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				select {
				case <-done:
					return
				case out <- Event{
					Time:  now,
					Key:   c.Key,
					Value: c.Value(),
				}:
				}
			}
		}
	}()
	return out
}

type FloodConfig struct {
	Count  int
	Prefix string
	Period time.Duration
	Value  func() float64
}

func Flood(done <-chan struct{}, c FloodConfig) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		wg := sync.WaitGroup{}
		wg.Add(c.Count)
		for i := 0; i < c.Count; i++ {
			go func(id int) {
				defer wg.Done()
				events := Generate(done, GenConfig{
					Key:    fmt.Sprintf("%s.%d", c.Prefix, id),
					Period: c.Period,
					Value:  c.Value,
				})
				for event := range events {
					out <- event
				}
			}(i)
		}
		wg.Wait()
	}()
	return out
}
