package main

import (
	"flag"
	"math/rand"
	"sync"
	"time"

	. "github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var sid = flag.Int("sid", 0, "Zone ID.")
var nid = flag.Int("nid", 0, "Node ID.")
var T = flag.Int("T", 1, "Number of threads (simulated clients).")
var addr = flag.String("master", "127.0.0.1", "Master address.")

var t = flag.Int("t", 0, "Total number of time in seconds.")
var n = flag.Int("n", 1000, "Total number of requests.")
var rounds = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var synch = flag.Bool("sync", true, "Sync operations.")
var writes = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")

var keys = flag.Int("k", 1000, "Key space")
var distribution = flag.String("d", "random", "Key distribution")
var mu = flag.Float64("mu", 0, "Mu of normal distribution")
var sigma = flag.Float64("sigma", 60, "Sigma of normal distribution")
var move = flag.Bool("move", false, "Moving average (mu)")
var zs = flag.Float64("zs", 2, "Zipfian s parameter")
var zv = flag.Float64("zv", 1, "Zipfian v parameter")
var conflicts = flag.Int("c", 100, "Percentage of conflicts")
var throttle = flag.Int("throttle", 1000, "requests per second throttle")

var counter = 0
var throttleUnit = *throttle / 100
var prevAsyncThrottleTime int64

var cwait sync.WaitGroup
var latency = make([]time.Duration, 0)
var min int
var zipf *rand.Zipf

func do(client *Client, done chan<- bool) {
	var key Key
	switch *distribution {
	case "normal":
		k := int(rand.NormFloat64()**sigma + *mu)
		for k < 0 {
			k += *keys
		}
		for k > *keys {
			k -= *keys
		}
		key = Key(k)
	case "zipfan":
		key = Key(zipf.Uint64())
	case "random":
		if rand.Intn(100) < *conflicts {
			key = Key(rand.Intn(*keys))
		} else {
			key = Key(rand.Intn(*keys) + min)
		}
	}

	if *synch {
		start := time.Now()
		client.Put(key, []byte{42})
		t := time.Now().Sub(start)
		latency = append(latency, t)
		log.Infof("%v, %d, %v", client.ID, key, float64(t.Nanoseconds())/1000000.0)
	} else {
		client.PutAsync(key, []byte{41})
		counter++
		if counter%throttleUnit == 0 {
			tnow := time.Now().UnixNano()
			tdiff := tnow - prevAsyncThrottleTime + 1000
			treq := int64(1000000000 / *throttle) * int64(throttleUnit)
			// Debugf("Client %v throttles down. Sleeping %f ms", client.ID, float64((treq-tdiff)/1000000.0))
			time.Sleep(time.Duration(treq - tdiff))
			prevAsyncThrottleTime = time.Now().UnixNano()
		}
	}
	done <- true
}

func run(client *Client) {
	min = *sid * *keys
	seed := int64(*sid<<8 + *nid)
	zipf = rand.NewZipf(rand.New(rand.NewSource(seed)), *zs, *zv, uint64(*keys))
	rand.Seed(seed)

	done := make(chan bool, 1)
	if *t > 0 {
		timer := time.NewTimer(time.Second * time.Duration(*t))
		for {
			go do(client, done)
			select {
			case <-timer.C:
				client.Stop()
				cwait.Done()
				return

			case <-done:
			}
		}
	} else {
		for i := 0; i < *n; i++ {
			go do(client, done)
			<-done
		}
		client.Wait()
		cwait.Done()
	}
}

func main() {
	flag.Parse()

	config := ConnectToMaster(*addr, true, NewID(uint8(*sid), uint8(*nid)))
	log.Infof("Received config %s\n", config)

	clients := make([]*Client, 0)

	for i := 1; i <= *T; i++ {
		config.ID = NewID(uint8(*sid), uint8(i))
		log.Infof("Client %s config %s\n", config.ID, config)
		c := NewClient(config)
		c.Start()
		clients = append(clients, c)
	}

	start_time := time.Now()
	prevAsyncThrottleTime = start_time.UnixNano()

	var stop chan bool
	if *move {
		move := func() { *mu = float64(int(*mu+1) % *keys) }
		stop = Schedule(move, 500*time.Millisecond)
	}

	for j := 0; j < *rounds; j++ {
		start := time.Now()

		for i := 0; i < *T; i++ {
			cwait.Add(1)
			go run(clients[i])
		}
		cwait.Wait()
		end := time.Now()

		if *synch {
			stat := Statistic(latency)
			stat.WriteFile("latency")

			log.Infof("Round took %v\n", end.Sub(start))
			log.Infof("throughput %f\n", float64(len(latency))/end.Sub(start).Seconds())
			log.Warningln(stat)

			latency = make([]time.Duration, 0)
		} else {
			log.Infof("Total async operations: %d", counter)
		}

	}

	end_time := time.Now()
	log.Infof("Test took %v\n", end_time.Sub(start_time))

	if *move {
		stop <- true
	}

}
