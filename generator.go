package meb

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

type Generator struct {
	From   time.Time
	Until  time.Time
	Step   time.Duration
	Prefix string
	IDs    []string
	Value  func() float64
}

func (g Generator) Generate() <-chan Event {
	out := make(chan Event)
	done := make(chan struct{})
	var count int64
	go func() {
		defer close(out)
		defer close(done)
		for t := g.From; t.Before(g.Until); t = t.Add(g.Step) {
			for _, id := range g.IDs {
				key := fmt.Sprintf("%s.%s", g.Prefix, id)
				out <- Event{
					Time:  t,
					Key:   key,
					Value: g.Value(),
				}
				atomic.AddInt64(&count, 1)
			}
		}
	}()
	go func() {
		c := time.Tick(time.Second)
		for {
			select {
			case <-done:
				return
			case <-c:
				log.Printf("generated %d events", count)
			}
		}
	}()
	return out
}
