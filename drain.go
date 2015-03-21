package meb

import (
	"sync"

	"gopkg.in/mgo.v2"
)

type Drainer struct {
	URL         string
	Database    string
	Collection  string
	Concurrency int
}

func (d Drainer) Drain(events <-chan Event) error {
	session, err := mgo.Dial(d.URL)
	if err != nil {
		return err
	}
	coll := session.DB(d.Database).C(d.Collection)
	wg := sync.WaitGroup{}
	wg.Add(d.Concurrency)
	batches := batchEvents(100, events)
	var once sync.Once
	var outerErr error
	for i := 0; i < d.Concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			if outerErr != nil {
				return
			}
			b := coll.Bulk()
			for batch := range batches {
				for _, event := range batch {
					b.Insert(event)
				}
				_, err := b.Run()
				if err != nil {
					once.Do(func() {
						outerErr = err
					})
				}
			}
		}(i)
	}
	wg.Wait()
	return outerErr
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
