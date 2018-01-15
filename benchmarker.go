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
	T                    int    // total number of running time in seconds
	N                    int    // total number of requests
	K                    int    // key sapce
	W                    int    // percentage of writes
	Concurrency          int    // number of simulated clients
	Distribution         string // distribution
	LinearizabilityCheck bool   // run linearizability checker at the end of benchmark
	// rounds       int    // repeat in many rounds sequentially

	// random distribution
	Conflicts int // percetage of conflicting keys
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	Zipfian_s float64 // zipfian s parameter
	Zipfian_v float64 // zipfian v parameter

	Throttle int // requests per second throttle
}

func NewBenchmarkConfig() bconfig {
	return bconfig{
		T:                    10,
		N:                    0,
		K:                    1000,
		W:                    100,
		Concurrency:          1,
		Distribution:         "random",
		LinearizabilityCheck: false,
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		Zipfian_s:            2,
		Zipfian_v:            1,
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

// Save saves the benchmark parameters to configuration file
func (c *bconfig) Save() error {
	f, err := os.Create(*file)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(f)
	return encoder.Encode(c)
}

type Benchmarker struct {
	db DB // read/write operation interface
	bconfig
	History

	cwait   sync.WaitGroup  // wait for all clients to finish
	latency []time.Duration // latency per operation for each round
	zipf    *rand.Zipf
}

func NewBenchmarker(db DB) *Benchmarker {
	b := new(Benchmarker)
	b.db = db
	b.bconfig = NewBenchmarkConfig()
	b.History = NewHistory()
	return b
}

// Run starts the main logic of benchmarking
func (b *Benchmarker) Run() {
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.Zipfian_s, b.Zipfian_v, uint64(b.K))

	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	b.latency = make([]time.Duration, 1000)
	start_time := time.Now()
	b.db.Init()

	keys := make(chan int, b.Concurrency)
	results := make(chan time.Duration, 1000)
	defer close(results)
	go b.collect(results)
	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, results)
	}
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T))
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
		for i := 0; i < b.N; i++ {
			keys <- b.next()
		}
	}

	b.db.Stop()
	end_time := time.Now()
	close(keys)
	stat := Statistic(b.latency)
	stat.WriteFile("latency")
	t := end_time.Sub(start_time)
	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Infoln(stat)

	if b.LinearizabilityCheck {
		if b.History.Linearizable() {
			log.Infoln("The execution is linearizable.")
		} else {
			log.Infoln("The execution is NOT linearizable.")
		}
	}
}

// generates key based on distribution
func (b *Benchmarker) next() int {
	var key int
	switch b.Distribution {
	case "random":
		if rand.Intn(100) < b.Conflicts {
			key = rand.Intn(b.K)
		} else {
			key = rand.Intn(b.K) + b.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.Sigma + b.Mu)
		for key < 0 {
			key += b.K
		}
		for key > b.K {
			key -= b.K
		}

	case "zipfan":
		key = int(b.zipf.Uint64())
	}

	return key
}

func (b *Benchmarker) worker(keys <-chan int, results chan<- time.Duration) {
	for k := range keys {
		var s time.Time
		var e time.Time
		if rand.Intn(100) < b.W {
			v := rand.Int()
			s = time.Now()
			b.db.Write(k, v)
			e = time.Now()
			b.History.Add(k, v, nil, s.UnixNano(), e.UnixNano())
		} else {
			s = time.Now()
			v := b.db.Read(k)
			e = time.Now()
			b.History.Add(k, nil, v, s.UnixNano(), e.UnixNano())
		}
		t := e.Sub(s)
		results <- t
	}
}

func (b *Benchmarker) collect(results <-chan time.Duration) {
	for t := range results {
		b.latency = append(b.latency, t)
	}
}
