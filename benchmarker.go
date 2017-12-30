package paxi

import (
	"encoding/json"
	"flag"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

type DB interface {
	Init()
	Read(key int) int
	Write(key, value int)
	Stop()
}

var file = flag.String("bconfig", "benchmark.json", "benchmark configuration file")

type bconfig struct {
	t            int    // total number of running time in seconds
	n            int    // total number of requests
	k            int    // key sapce
	w            int    // percentage of writes
	concurrency  int    // number of simulated clients
	distribution string // distribution
	// rounds       int    // repeat in many rounds sequentially

	// random distribution
	conflicts int // percetage of conflicting keys
	min       int // min key

	// normal distribution
	mu    float64 // mu of normal distribution
	sigma float64 // sigma of normal distribution
	move  bool    // moving average (mu) of normal distribution
	speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	zipfian_s float64 // zipfian s parameter
	zipfian_v float64 // zipfian v parameter

	throttle int // requests per second throttle
}

func NewBenchmarkConfig() bconfig {
	return bconfig{
		t:            10,
		n:            0,
		k:            1000,
		w:            100,
		concurrency:  1,
		distribution: "random",
		conflicts:    100,
		min:          0,
		mu:           0,
		sigma:        60,
		move:         false,
		speed:        500,
		zipfian_s:    2,
		zipfian_v:    1,
	}
}

// Load reads the benchmark parameters from configuration file
func (c *bconfig) Load() error {
	f, err := os.Open(*file)
	if err != err {
		return err
	}
	decoder := json.NewDecoder(f)
	return decoder.Decode(c)
}

type Benchmarker struct {
	db DB // read/write operation interface
	bconfig

	cwait   sync.WaitGroup  // wait for all clients to finish
	latency []time.Duration // latency per operation for each round
	zipf    *rand.Zipf
}

func NewBenchmarker(db DB) *Benchmarker {
	b := new(Benchmarker)
	b.db = db
	b.bconfig = NewBenchmarkConfig()
	return b
}

// Start starts the main logic of benchmarking
func (b *Benchmarker) Start() {
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.zipfian_s, b.zipfian_v, uint64(b.k))

	var stop chan bool
	if b.move {
		move := func() { b.mu = float64(int(b.mu+1) % b.k) }
		stop = Schedule(move, time.Duration(b.speed)*time.Millisecond)
	}

	b.latency = make([]time.Duration, 1000)
	start_time := time.Now()
	b.db.Init()

	keys := make(chan int, 1000)
	results := make(chan time.Duration, 1000)
	go b.collect(results)
	for i := 0; i < b.concurrency; i++ {
		go b.worker(keys, results)
	}
	if b.t > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.t))
	loop:
		for {
			select {
			case <-timer.C:
				break loop
			default:
				keys <- b.next()
			}
		}
	} else {
		for i := 0; i < b.n; i++ {
			keys <- b.next()
		}
	}

	b.db.Stop()
	end_time := time.Now()
	stat := Statistic(b.latency)
	stat.WriteFile("latency")
	t := end_time.Sub(start_time)
	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Infoln(stat)

	if b.move {
		stop <- true
	}
	close(keys)
	close(results)
}

// generates key based on distribution
func (b *Benchmarker) next() int {
	var key int
	switch b.distribution {
	case "random":
		if rand.Intn(100) < b.conflicts {
			key = rand.Intn(b.k)
		} else {
			key = rand.Intn(b.k) + b.min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.sigma + b.mu)
		for key < 0 {
			key += b.k
		}
		for key > b.k {
			key -= b.k
		}

	case "zipfan":
		key = int(b.zipf.Uint64())
	}

	return key
}

func (b *Benchmarker) worker(keys <-chan int, results chan<- time.Duration) {
	for k := range keys {
		v := rand.Int()

		s := time.Now()
		if rand.Intn(100) < b.w {
			b.db.Write(k, v)
		} else {
			b.db.Read(k)
		}
		t := time.Now().Sub(s)
		results <- t
	}
}

func (b *Benchmarker) collect(results <-chan time.Duration) {
	for t := range results {
		b.latency = append(b.latency, t)
	}
}
