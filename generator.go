package meb

import (
	"fmt"
	"log"
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
	go func() {
		defer close(out)
		count := 0
		for t := g.From; t.Before(g.Until); t = t.Add(g.Step) {
			for _, id := range g.IDs {
				key := fmt.Sprintf("%s.%s", g.Prefix, id)
				out <- Event{
					Time:  t,
					Key:   key,
					Value: g.Value(),
				}
				count++
				if count%1000 == 0 {
					log.Printf("generated %d events", count)
				}
			}
		}
	}()
	return out
}
