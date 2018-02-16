package main

import (
	"flag"
	"sync"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/atomic"
	"github.com/ailidani/paxi/kpaxos"
	"github.com/ailidani/paxi/log"
	"github.com/ailidani/paxi/paxos"
	"github.com/ailidani/paxi/paxos_group"
	"github.com/ailidani/paxi/ppaxos"
	"github.com/ailidani/paxi/wpaxos"
)

var id = flag.String("id", "", "ID in format of Zone.Node.")

var master = flag.String("master", "", "Master address.")

func replica(id paxi.ID) {
	if *master != "" {
		paxi.ConnectToMaster(*master, false)
	}

	log.Infof("node %v starting...", id)

	switch paxi.Config.Algorithm {

	case "paxos":
		paxos.NewReplica(id).Run()

	case "wpaxos":
		wpaxos.NewReplica(id).Run()

	// case "epaxos":
	// 	replica := epaxos.NewReplica(id)
	// 	replica.Run()

	case "kpaxos":
		kpaxos.NewReplica(id).Run()

	case "paxos_groups":
		paxos_group.NewReplica(id).Run()

	case "atomic":
		atomic.NewReplica(id).Run()

	case "ppaxos":
		ppaxos.NewReplica(id).Run()

	default:
		panic("Unknown algorithm.")
	}
}

func main() {
	flag.Parse()
	log.Setup()
	paxi.Config.Load()

	if paxi.Config.Transport == "chan" {
		var wg sync.WaitGroup
		wg.Add(1)
		for id := range paxi.Config.Addrs {
			go func(n paxi.ID) {
				replica(n)
			}(id)
		}
		wg.Wait()
	} else {
		replica(paxi.ID(*id))
	}
}
