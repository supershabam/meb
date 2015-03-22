package meb

import (
	"log"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	"gopkg.in/mgo.v2"
)

type Drainer struct {
	URL         string
	Database    string
	Collection  string
	Concurrency int
	Batch       int
}

func (d Drainer) Drain(events <-chan Event) (metrics.Histogram, error) {
	h := metrics.NewHistogram(metrics.NewExpDecaySample(4098, 0.015))
	done := make(chan struct{})
	defer close(done)
	session, err := mgo.Dial(d.URL)
	if err != nil {
		return h, err
	}
	coll := session.DB(d.Database).C(d.Collection)
	wg := sync.WaitGroup{}
	wg.Add(d.Concurrency)
	batches := batchEvents(d.Batch, events)
	var once sync.Once
	var outerErr error
	for i := 0; i < d.Concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			if outerErr != nil {
				return
			}
			for batch := range batches {
				b := coll.Bulk()
				b.Unordered()
				for _, event := range batch {
					b.Insert(event)
				}
				start := time.Now()
				_, err := b.Run()
				if err != nil {
					once.Do(func() {
						outerErr = err
					})
				}
				end := time.Now()
				ms := end.Sub(start).Nanoseconds() / 1e6
				h.Update(ms)
			}
		}(i)
	}
	go func() {
		c := time.Tick(time.Second)
		for {
			select {
			case <-done:
				return
			case <-c:
				log.Printf("wrote %d events with average %.2fms write time", h.Count(), h.Percentile(0.50))
			}
		}
	}()
	wg.Wait()
	return h, outerErr
}

func batchEvents(size int, in <-chan Event) <-chan []Event {
	out := make(chan []Event)
	go func() {
		defer close(out)
		batch := make([]Event, 0, size)
		for event := range in {
			batch = append(batch, event)
			if len(batch) == size {
				out <- batch
				batch = make([]Event, 0, size)
			}
		}
		if len(batch) != 0 {
			out <- batch
		}
	}()
	return out
}
