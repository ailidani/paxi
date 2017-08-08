package main

import (
	"flag"
	"math/rand"
	. "paxi"
	"paxi/glog"
	"sync"
	"time"
)

var sid = flag.Int("sid", 0, "Site ID.")
var nid = flag.Int("nid", 0, "Node ID.")
var T = flag.Int("T", 1, "Number of threads (simulated clients).")
var addr = flag.String("master", "127.0.0.1", "Master address.")

var t = flag.Int("t", 0, "Total number of time in seconds.")
var rounds = flag.Int("r", 1, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var synch = flag.Bool("sync", false, "Sync operations.")
var writes = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")

var keys = flag.Int("k", 1000, "Key space")
var distribution = flag.String("d", "normal", "Key distribution")
var mu = flag.Float64("mu", 0, "Mu of normal distribution")
var sigma = flag.Float64("sigma", 60, "Sigma of normal distribution")
var move = flag.Bool("move", false, "Moving average (mu)")
var zs = flag.Float64("zs", 2, "Zipfian s parameter")
var zv = flag.Float64("zv", 1, "Zipfian v parameter")
var conflicts = flag.Int("c", 100, "Percentage of conflicts")
var throttle = flag.Int("th", 1000, "requests per second throttle")

var counter = 0
var throttleUnit = *throttle / 100
var prevAsyncThrottleTime int64

var cwait sync.WaitGroup
var latency = make([]time.Duration, 0)
var min int
var zipf *rand.Zipf

func do(client *Client) {
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
	// glog.V(2).Infof("key=%d", key)

	if *synch {
		start := time.Now()
		client.Put(key, 42)
		t := time.Now().Sub(start)
		latency = append(latency, t)
		glog.V(1).Infof("%v, %d, %v", client.ID, key, float64(t.Nanoseconds())/1000000.0)
	} else {
		client.PutAsync(key, 41)
		counter++
		if counter%throttleUnit == 0 {
			tnow := time.Now().UnixNano()
			tdiff := tnow - prevAsyncThrottleTime + 1000
			treq := int64(1000000000 / *throttle) * int64(throttleUnit)
			// glog.V(2).Infof("Client %v throttles down. Sleeping %f ms", client.ID, float64((treq-tdiff)/1000000.0))
			time.Sleep(time.Duration(treq - tdiff))
			prevAsyncThrottleTime = time.Now().UnixNano()
		}
	}
}

func run(client *Client) {
	min = *sid * *keys
	seed := int64(*sid<<8 + *nid)
	zipf = rand.NewZipf(rand.New(rand.NewSource(seed)), *zs, *zv, uint64(*keys))
	rand.Seed(seed)

	if *t > 0 {
		timer := time.NewTimer(time.Second * time.Duration(*t))
		for {
			select {
			case <-timer.C:
				client.Stop()
				cwait.Done()
				return

			default:
				do(client)
			}
		}
	} else {
		for i := 0; i < *keys; i++ {
			do(client)
		}
		client.Wait()
		cwait.Done()
	}
}

func main() {
	flag.Parse()

	config := ConnectToMaster(*addr, CLIENT, NewID(uint8(*sid), uint8(*nid)))
	glog.Infof("Received config %s\n", config)

	clients := make([]*Client, 0)

	for i := 1; i <= *T; i++ {
		config.ID = NewID(uint8(*sid), uint8(i))
		glog.Infof("Client %s config %s\n", config.ID, config)
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
			// glog.Warningln(stat.Data)
			stat.WriteFile("latency")

			glog.Warningf("Round took %v\n", end.Sub(start))
			glog.Warningf("throughput %f\n", float64(len(latency))/end.Sub(start).Seconds())
			glog.Warningln(stat)

			latency = make([]time.Duration, 0)
		} else {
			glog.Warningf("Total async operations: %d", counter)
		}

	}

	end_time := time.Now()
	glog.Warningf("Test took %v\n", end_time.Sub(start_time))
	glog.Flush()

	if *move {
		stop <- true
	}

}
