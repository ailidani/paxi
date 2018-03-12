package paxi

import (
	"flag"
	"math/rand"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// DB is general interface implemented by client to call client library
type DB interface {
	Init()
	Read(key int) int
	Write(key, value int)
	Stop()
}

var file = flag.String("bconfig", "benchmark.json", "benchmark configuration file")

type bconfig struct {
	T                    int     // total number of running time in seconds
	N                    int     // total number of requests
	K                    int     // key sapce
	W                    float64 // write ratio
	Throttle             int     // requests per second throttle, unused if 0
	Concurrency          int     // number of simulated clients
	Distribution         string  // distribution
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	// rounds       int    // repeat in many rounds sequentially

	// random distribution
	Conflicts int // percentage of conflicting keys
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianS float64 // zipfian s parameter
	ZipfianV float64 // zipfian v parameter
}

// defaultBConfig returns a default benchmark config
func defaultBConfig() bconfig {
	return bconfig{
		T:                    60,
		N:                    0,
		K:                    1000,
		W:                    50,
		Throttle:             0,
		Concurrency:          1,
		Distribution:         "uniform",
		LinearizabilityCheck: true,
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		ZipfianS:             2,
		ZipfianV:             1,
	}
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	db DB // read/write operation interface
	bconfig
	*History

	cwait     sync.WaitGroup  // wait for all clients to finish
	latency   []time.Duration // latency per operation
	startTime time.Time
	zipf      *rand.Zipf
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(db DB) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.bconfig = config.Benchmark
	b.History = NewHistory()
	return b
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.ZipfianS, b.ZipfianV, uint64(b.K))

	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	b.latency = make([]time.Duration, 0)
	b.db.Init()
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	b.startTime = time.Now()
	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
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
	t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	stat := Statistic(b.latency)

	log.Infof("Benchmark took %v\n", t)
	log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	log.Info(stat)

	stat.WriteFile("latency")
	b.History.WriteFile("history")

	if b.LinearizabilityCheck {
		n := b.History.Linearizable()
		if n == 0 {
			log.Info("The execution is linearizable.")
		} else {
			log.Info("The execution is NOT linearizable.")
			log.Infof("Total anomaly read operations are %d", n)
			log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
		}
	}
}

// generates key based on distribution
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "uniform":
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

func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	if b.Throttle > 0 {
		t := NewLimiter(b.Throttle)
		for k := range keys {
			t.Wait()
			go b.do(k, result)
		}
	} else {
		for k := range keys {
			b.do(k, result)
		}
	}
}

// do one read or write operation
func (b *Benchmark) do(k int, result chan<- time.Duration) {
	var s time.Time
	var e time.Time
	if rand.Float64() < b.W {
		v := rand.Int()
		s = time.Now()
		b.db.Write(k, v)
		e = time.Now()
		b.History.Add(k, v, nil, s.Sub(b.startTime).Nanoseconds(), e.Sub(b.startTime).Nanoseconds())
	} else {
		s = time.Now()
		v := b.db.Read(k)
		e = time.Now()
		b.History.Add(k, nil, v, s.Sub(b.startTime).Nanoseconds(), e.Sub(b.startTime).Nanoseconds())
	}
	t := e.Sub(s)
	result <- t
}

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
	}
}
