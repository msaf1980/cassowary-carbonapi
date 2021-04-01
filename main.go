package main

import (
	"encoding/csv"
	"flag"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	cassowary "github.com/msaf1980/cassowary/pkg/client"
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

	users_1_hour := flag.Int("users_1_hour", 10, "Users for 1 hour queries")
	users_1_day := flag.Int("users_1_day", 2, "Users for 1 day queries")
	users_1_week := flag.Int("users_1_week", 2, "Users for 1 week queries")
	users_3_month := flag.Int("users_3_month", 1, "Users for 3 months queries")
	users_1_year := flag.Int("users_1_year", 0, "Users for 1 year queries")

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

	it_1_hour := NewURLIterator(3600, targets)
	it_1_day := NewURLIterator(86400, targets)
	it_1_week := NewURLIterator(604800, targets)
	it_3_month := NewURLIterator(7776000, targets)
	it_1_year := NewURLIterator(31536000, targets)

	cass := &cassowary.Cassowary{
		BaseURL:  *baseURL,
		Duration: *duration,
		Groups: []cassowary.QueryGroup{
			{
				Name:             "1 Hour",
				ConcurrencyLevel: *users_1_hour,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_1_hour,
			},
			{
				Name:             "1 Day",
				ConcurrencyLevel: *users_1_day,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_1_day,
			},
			{
				Name:             "1 Week",
				ConcurrencyLevel: *users_1_week,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_1_week,
			},
			{
				Name:             "3 Month",
				ConcurrencyLevel: *users_3_month,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_3_month,
			},
			{
				Name:             "1 Year",
				ConcurrencyLevel: *users_1_year,
				Delay:            *delay,
				FileMode:         true,
				URLIterator:      it_1_year,
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
