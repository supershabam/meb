package main

import (
	"flag"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/supershabam/meb"
)

var (
	prefix     = flag.String("prefix", "droplet.cpu", "prefix for the keys")
	from       = flag.Duration("from", -time.Hour*2, "relative start time")
	until      = flag.Duration("until", time.Millisecond, "relative end time")
	step       = flag.Duration("step", time.Second, "time between values for a metric")
	ids        = flag.Int("ids", 1000, "number of metric ids to generate")
	url        = flag.String("url", "mongodb://localhost", "mongodb url")
	database   = flag.String("database", "events", "mongodb database")
	collection = flag.String("collection", "events", "mongodb collection")
)

func makeIDs(ids int) []string {
	out := []string{}
	for i := 0; i < ids; i++ {
		out = append(out, strconv.Itoa(i))
	}
	return out
}

func main() {
	flag.Parse()
	g := meb.Generator{
		From:   time.Now().Add(*from),
		Until:  time.Now().Add(*until),
		Step:   *step,
		Prefix: *prefix,
		IDs:    makeIDs(*ids),
		Value:  func() float64 { return rand.NormFloat64() },
	}
	d := meb.Drainer{
		URL:         *url,
		Database:    *database,
		Collection:  *collection,
		Concurrency: 2,
	}
	events := g.Generate()
	err := d.Drain(events)
	if err != nil {
		log.Fatal(err)
	}
}
