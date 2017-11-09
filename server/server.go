package main

import (
	"flag"
	"log"
	. "paxi"
	"paxi/cosmos"
	"paxi/epaxos"
	"paxi/glog"
	"paxi/kpaxos"
	"paxi/wpaxos"
	"strconv"
	"sync"
)

var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")
var sid = flag.Int("sid", 0, "Site ID. Default 0.")
var nid = flag.Int("nid", 0, "Node ID. Default 0.")
var master = flag.String("master", "127.0.0.1", "Master address.")

var mock = flag.Bool("mock", false, "Mocking network by chan and goroutine.")
var n = flag.Int("n", 3, "number of servers in each site")
var m = flag.Int("m", 2, "number of sites")

// mocking configuration with n servers in m sites
func mockConfigs(n int, m int) []*Config {
	addrs := make(map[ID]string, n*m)
	http := make(map[ID]string, n*m)
	p := 0
	for i := 0; i < m; i++ {
		for j := 0; j < n; j++ {
			id := NewID(uint8(i), uint8(j))
			addrs[id] = "chan://127.0.0.1:" + strconv.Itoa(PORT+p)
			http[id] = "http://127.0.0.1:" + strconv.Itoa(HTTP_PORT+p)
			p++
		}
	}

	configs := make([]*Config, 0)
	for id, _ := range addrs {
		c := MakeDefaultConfig()
		c.Algorithm = "cosmos"
		c.ID = id
		c.Addrs = addrs
		c.HTTPAddrs = http
		configs = append(configs, c)
	}

	return configs
}

func replica(id ID) {
	config := ConnectToMaster(*master, false, id)
	glog.Infof("server %v received config from master\n", config.ID)

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

	if *mock {
		var wg sync.WaitGroup
		wg.Add(1)
		mockNodes()
		wg.Wait()
	}

	replica(NewID(uint8(*sid), uint8(*nid)))

}