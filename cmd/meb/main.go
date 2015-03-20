package main

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/supershabam/meb"
)

var (
	prefix      = flag.String("prefix", "droplet.cpu", "prefix for the keys")
	count       = flag.Int("count", 10e3, "number of event generators")
	period      = flag.Duration("period", time.Second, "period to generate events at")
	url         = flag.String("url", "mongodb://localhost", "mongodb url")
	database    = flag.String("database", "events", "mongodb database")
	collection  = flag.String("collection", "events", "mongodb collection")
	concurrency = flag.Int("concurrency", 20, "write concurrency")
)

func main() {
	flag.Parse()
	done := make(chan struct{})
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		<-c
		close(done)
	}()
	events := meb.Flood(done, meb.FloodConfig{
		Count:  *count,
		Prefix: *prefix,
		Period: *period,
		Value:  func() float64 { return rand.NormFloat64() },
	})
	err := meb.Drain(events, meb.DrainConfig{
		URL:         *url,
		Database:    *database,
		Collection:  *collection,
		Concurrency: *concurrency,
	})
	if err != nil {
		log.Fatal(err)
	}
}
