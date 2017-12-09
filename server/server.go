package main

import (
	"flag"
	"strconv"
	"sync"

	. "github.com/ailidani/paxi"
	"github.com/ailidani/paxi/cosmos"
	"github.com/ailidani/paxi/epaxos"
	"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/wpaxos"
)

var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")
var sid = flag.Int("sid", 0, "Site ID. Default 0.")
var nid = flag.Int("nid", 0, "Node ID. Default 0.")
var master = flag.String("master", "", "Master address.")

var simulation = flag.Bool("simulation", false, "Mocking network by chan and goroutine.")
var n = flag.Int("n", 3, "number of servers in each zone")
var m = flag.Int("m", 3, "number of zones")

func replica(id ID) {
	var config Config
	if *master == "" {
		config = *NewConfig(id, *configFile)
	} else {
		config = *ConnectToMaster(*master, false, id)
	}

	log.Infof("server %v started\n", config.ID)

	switch config.Algorithm {
	case "wpaxos":
		replica := wpaxos.NewReplica(config)
		replica.Run()

	case "epaxos":
		replica := epaxos.NewReplica(config)
		replica.Run()

	case "kpaxos":
		replica := kpaxos.NewReplica(config)
		replica.Run()

	case "cosmos":
		replica := cosmos.NewReplica(config)
		replica.Run()

	default:
		log.Fatalln("Unknown algorithm.")
	}
}

// not used
func mockConfig() Config {
	addrs := make(map[ID]string, *m**n)
	http := make(map[ID]string, *m**n)
	p := 0
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := NewID(uint8(i), uint8(j))
			addrs[id] = "chan://127.0.0.1:" + strconv.Itoa(PORT+p)
			http[id] = "http://127.0.0.1:" + strconv.Itoa(HTTP_PORT+p)
			p++
		}
	}

	c := MakeDefaultConfig()
	c.Addrs = addrs
	c.HTTPAddrs = http
	return *c
}

func mockNodes() {
	for i := 1; i <= *m; i++ {
		for j := 1; j <= *n; j++ {
			id := NewID(uint8(i), uint8(j))
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

	id := NewID(uint8(*sid), uint8(*nid))
	replica(id)
}
