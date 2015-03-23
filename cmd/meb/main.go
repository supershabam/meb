package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/supershabam/meb"
)

var (
	batch       = flag.Int("batch", 250, "number of inserts to batch into a single write")
	concurrency = flag.Int("concurrency", 20, "number of goroutines to write batches")
	prefix      = flag.String("prefix", "droplet.cpu", "prefix for the keys")
	from        = flag.Duration("from", -time.Second*2, "relative start time")
	until       = flag.Duration("until", time.Millisecond, "relative end time")
	step        = flag.Duration("step", time.Second, "time between values for a metric")
	ids         = flag.Int("ids", 3, "number of metric ids to generate")
	url         = flag.String("url", "mongodb://localhost", "mongodb url")
	database    = flag.String("database", "events", "mongodb database")
	collection  = flag.String("collection", "events", "mongodb collection")
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
	// d := meb.Drainer{
	// 	URL:         *url,
	// 	Database:    *database,
	// 	Collection:  *collection,
	// 	Concurrency: *concurrency,
	// 	Batch:       *batch,
	// }
	d := meb.PerCollectionDrainer{
		URL:         *url,
		Database:    *database,
		Concurrency: *concurrency,
	}
	start := time.Now()
	events := g.Generate()
	h, err := d.Drain(events)
	if err != nil {
		log.Fatal(err)
	}
	end := time.Now()
	fmt.Printf("%d total writes completed in %s\n", h.Count(), end.Sub(start))
	fmt.Println("write completion percentiles:")
	fmt.Printf("0.005 - %.2fms\n", h.Percentile(0.005))
	fmt.Printf("0.02  - %.2fms\n", h.Percentile(0.02))
	fmt.Printf("0.09  - %.2fms\n", h.Percentile(0.09))
	fmt.Printf("0.25  - %.2fms\n", h.Percentile(0.25))
	fmt.Printf("0.50  - %.2fms\n", h.Percentile(0.50))
	fmt.Printf("0.75  - %.2fms\n", h.Percentile(0.75))
	fmt.Printf("0.91  - %.2fms\n", h.Percentile(0.91))
	fmt.Printf("0.98  - %.2fms\n", h.Percentile(0.98))
	fmt.Printf("0.995 - %.2fms\n", h.Percentile(0.995))
}
