package benchmark

import (
	"encoding/json"
	"flag"
	"os"
)

var file = flag.String("config", "benchmark.json", "benchmark configuration file")

type config struct {
	t            uint   // total number of running time in seconds
	n            uint   // total number of requests
	k            uint   // key sapce
	w            uint   // percentage of writes
	concurrency  uint   // number of simulated clients
	rounds       uint   // repeat in many rounds sequentially
	distribution string // distribution

	// random distribution
	conflicts uint // percetage of conflicting keys

	// normal distribution
	mu    float64 // mu of normal distribution
	sigma float64 // sigma of normal distribution
	move  bool    // moving average (mu) of normal distribution

	// zipfian distribution
	zipfian_s float64 // zipfian s parameter
	zipfian_v float64 // zipfian v parameter

	throttle uint // requests per second throttle
}

func NewBenchmarkConfig() config {
	return config{
		t:            0,
		n:            1000,
		k:            1000,
		w:            100,
		concurrency:  1,
		rounds:       1,
		distribution: "random",
		conflicts:    0,
	}
}

func (c *config) Load() error {
	f, err := os.Open(*file)
	if err != err {
		return err
	}
	decoder := json.NewDecoder(f)
	return decoder.Decode(c)
}
