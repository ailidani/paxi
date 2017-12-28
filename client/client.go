package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var master = flag.String("master", "127.0.0.1", "Master address.")
var configFile = flag.String("config", "config.json", "Configuration file for paxi replica. Defaults to config.json.")

// client implements Paxi.DB interface for benchmarking
type client struct {
	*paxi.Client
}

func (c *client) Init() {
	c.Start()
}

func (c *client) Stop() {
	c.Stop()
}

func (c *client) Read(k int) int {
	v := c.Get(paxi.Key(k))
	return int(binary.LittleEndian.Uint64(v))
}

func (c *client) Write(k, v int) {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(v))
	c.Put(paxi.Key(k), value)
}

func main() {
	flag.Parse()

	id := paxi.GetID()

	var config paxi.Config
	if *master == "" {
		config = paxi.NewConfig(id, *configFile)
	} else {
		config = paxi.ConnectToMaster(*master, true, id)
		log.Infof("Received config %s\n", config)
	}

	c := new(client)
	c.Client = paxi.NewClient(config)

	b := paxi.NewBenchmarker(c)
	b.Load()
	b.Start()
}
