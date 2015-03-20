package meb

import "time"

// Event is a single document that can be saved to Mongo
type Event struct {
	Time  time.Time
	Key   string
	Value float64
}
