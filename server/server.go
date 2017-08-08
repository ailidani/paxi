package main

import (
	"flag"
	"log"
	. "paxi"
	"paxi/epaxos"
	"paxi/kpaxos"
	"paxi/wpaxos"
	"strconv"
)

var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")
var sid = flag.Int("sid", 0, "Site ID. Default 0.")
var nid = flag.Int("nid", 0, "Node ID. Default 0.")
var addr = flag.String("master", "127.0.0.1", "Master address.")

func makeConfig(n int) []*Config {
	addrs := make(map[ID]string, n)
	for i := 0; i < n; i++ {
		addr := "127.0.0.1:" + strconv.Itoa(PORT+i)
		addrs[NewID(uint8(i%3), uint8(i))] = addr
	}

	configs := make([]*Config, 0)
	for i := 0; i < n; i++ {
		config := MakeDefaultConfig()
		config.ID = NewID(uint8(i%3), uint8(i))
		config.Addrs = addrs
		config.ConfigFile = strconv.Itoa(i) + ".json"
		//config.Save()
		configs = append(configs, config)
	}

	return configs
}

func main() {
	flag.Parse()
	//flag.Lookup("logtostderr").Value.Set("true")
	//flag.Lookup("stderrthreshold").Value.Set("WARNING")
	//flag.Lookup("log_dir").Value.Set("logs")
	//flag.Lookup("alsologtostderr").Value.Set("true")

	// config := MakeDefaultConfig()
	// config.ConfigFile = *configFile
	// err := config.Load()
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	//configs := makeConfig(6)

	config := ConnectToMaster(*addr, NODE, NewID(uint8(*sid), uint8(*nid)))
	log.Println(config)

	switch config.Protocol {
	case WPaxos:
		replica := wpaxos.NewReplica(config)
		replica.Run()

	case EPaxos:
		replica := epaxos.NewReplica(config)
		replica.Run()

	case KPaxos:
		replica := kpaxos.NewReplica(config)
		replica.Run()

	default:
		log.Fatalln("No algorithm is specified.")
	}
}
