package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/epaxos"
	"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/paxos"
	"github.com/ailidani/paxi/wpaxos"
	"github.com/ailidani/paxi/wpaxos2"
)

var master = flag.String("master", "", "Master address.")

var simulation = flag.Bool("simulation", false, "Mocking network by chan and goroutine.")
var n = flag.Int("n", 3, "number of servers in each zone")
var m = flag.Int("m", 3, "number of zones")

func replica(id paxi.ID) {
	var config paxi.Config
	if *master == "" {
		config = paxi.NewConfig(id)
	} else {
		config = paxi.ConnectToMaster(*master, false, id)
	}

	if *simulation {
		config.Transport = "chan"
	}

	log.Infof("server %v started\n", config.ID)

	switch config.Algorithm {

	case "paxos":
		replica := paxos.NewReplica(config)
		replica.Run()

	case "wpaxos":
		replica := wpaxos.NewReplica(config)
		replica.Run()

	case "wpaxos2":
		replica := wpaxos2.NewReplica(config)
		replica.Run()

	case "epaxos":
		replica := epaxos.NewReplica(config)
		replica.Run()

	case "kpaxos":
		replica := kpaxos.NewReplica(config)
		replica.Run()

	default:
		log.Fatalln("Unknown algorithm.")
	}
}

// not used
func mockConfig() paxi.Config {
	addrs := make(map[paxi.ID]string, *m**n)
	http := make(map[paxi.ID]string, *m**n)
	p := 0
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := paxi.ID(fmt.Sprintf("%d.%d", i, j))
			addrs[id] = "127.0.0.1:" + strconv.Itoa(paxi.PORT+p)
			http[id] = "http://127.0.0.1:" + strconv.Itoa(paxi.HTTP_PORT+p)
			p++
		}
	}

	c := paxi.MakeDefaultConfig()
	c.Addrs = addrs
	c.HTTPAddrs = http
	return c
}

func mockNodes() {
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := paxi.ID(fmt.Sprintf("%d.%d", i, j))
			go replica(id)
		}
	}
}

func main() {
	flag.Parse()

	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		mockNodes()
		wg.Wait()
	}

	id := paxi.GetID()
	replica(id)
}
