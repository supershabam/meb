package meb

import (
	"log"
	"sync"

	"gopkg.in/mgo.v2"
)

type DrainConfig struct {
	URL         string
	Database    string
	Collection  string
	Concurrency int
}

func Drain(events <-chan Event, c DrainConfig) error {
	session, err := mgo.Dial(c.URL)
	if err != nil {
		return err
	}
	coll := session.DB(c.Database).C(c.Collection)
	wg := sync.WaitGroup{}
	wg.Add(c.Concurrency)
	batches := batchEvents(100, events)
	var outerErr error
	for i := 0; i < c.Concurrency; i++ {
		go func(id int) {
			defer wg.Done()
			if outerErr != nil {
				return
			}
			b := coll.Bulk()
			for batch := range batches {
				b.Insert(batch)
				_, err := b.Run()
				if err != nil {
					outerErr = err
				}
				log.Printf("wrote %d events", len(batch))
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
