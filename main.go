package main

import (
	"encoding/csv"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	cassowary "github.com/msaf1980/cassowary/pkg/client"
	flag "github.com/spf13/pflag"
)

type URLIterator struct {
	pos      uint64
	data     [][]string
	duration int64 // duration beetween from/until (s)
}

func (it *URLIterator) Next() *cassowary.Query {
	for {
		pos := atomic.AddUint64(&it.pos, 1)
		if pos > uint64(len(it.data)) {
			if !atomic.CompareAndSwapUint64(&it.pos, pos, 0) {
				// retry
				continue
			}
			pos = 0
		} else {
			pos--
		}

		until := rand.Int63n(86399) // random in day range
		from := until + it.duration

		url := "/render/?format=protobuf" + it.data[pos][0] + "&from=" + "now-" + strconv.FormatInt(from, 10) + "s&until=" + "now-" + strconv.FormatInt(until, 10) + "s"

		return &cassowary.Query{Method: "GET", URL: url}
	}
}

func NewURLIterator(duration int64, data [][]string) *URLIterator {
	if len(data) == 0 {
		return nil
	}
	return &URLIterator{data: data, duration: duration, pos: 0}
}

func main() {
	targetsFile := flag.String("targets", "test.csv", "CSV file, at this time with one field - target")
	baseURL := flag.String("base", "http://127.0.0.1:8889", "Base URL")

	statFile := flag.String("status_file", "", "Statistic file for individual queries")
	jsonFile := flag.String("json_file", "", "JSON file for aggregated statistic")

	delay := flag.Duration("delay", 100*time.Millisecond, "Delay between requests")
	duration := flag.Duration("duration", time.Minute, "Duration")

	render_1_hour := flag.Int("render_1h", 8, "Users for 1 hour render queries")
	render_1_day := flag.Int("render_1d", 2, "Users for 1 day render queries")
	render_5_day := flag.Int("render_5d", 2, "Users for 5 days render queries")
	render_30_day := flag.Int("render_30d", 0, "Users for 30 days render queries")
	render_90_day := flag.Int("render_90d", 0, "Users for 90 days render queries")
	render_365_day := flag.Int("render_365d", 0, "Users for 365 days render queries")

	flag.Parse()

	file, err := os.Open(*targetsFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 1
	reader.Comment = '#'

	targets, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	targets = targets[1:]

	it_render_1_hour := NewURLIterator(3600, targets)
	it_render_1_day := NewURLIterator(86400, targets)
	it_render_5_day := NewURLIterator(518400, targets)
	it_render_30_day := NewURLIterator(2592000, targets)
	it_render_90_day := NewURLIterator(7776000, targets)
	it_render_365_day := NewURLIterator(31536000, targets)

	cass := &cassowary.Cassowary{
		BaseURL:  *baseURL,
		Duration: *duration,
		Groups: []cassowary.QueryGroup{
			{
				Name:             "Render 1 Hour",
				ConcurrencyLevel: *render_1_hour,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_1_hour,
			},
			{
				Name:             "Render 1 Day",
				ConcurrencyLevel: *render_1_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_1_day,
			},
			{
				Name:             "Render 6 Day",
				ConcurrencyLevel: *render_5_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_5_day,
			},
			{
				Name:             "Render 30 Day",
				ConcurrencyLevel: *render_30_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_30_day,
			},
			{
				Name:             "Render 90 Day",
				ConcurrencyLevel: *render_90_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_90_day,
			},
			{
				Name:             "Render 365 Day",
				ConcurrencyLevel: *render_365_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_render_365_day,
			},
		},
		DisableTerminalOutput: false,
		StatFile:              *statFile,
	}
	metrics, metricsGroup, err := cass.Coordinate()
	if err != nil {
		panic(err)
	}

	for _, g := range cass.Groups {
		if m, ok := metricsGroup[g.Name]; ok {
			cassowary.OutPutResults(m)
		}
	}
	cassowary.OutPutResults(metrics)

	if *jsonFile != "" {
		err = cassowary.OutPutJSON(*jsonFile, metrics, metricsGroup)
		if err != nil {
			log.Fatal(err)
		}
	}
}
