package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	c *paxi.Client
}

func (d *db) Init() {
	d.c.Start()
}

func (d *db) Stop() {
	d.c.Stop()
}

func (d *db) Read(k int) int {
	v := d.c.Get(paxi.Key(k))
	if len(v) == 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(v))
}

func (d *db) Write(k, v int) {
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(v))
	d.c.Put(paxi.Key(k), value)
}

func main() {
	flag.Parse()

	id := paxi.GetID()

	var config paxi.Config
	if *master == "" {
		config = paxi.NewConfig(id)
	} else {
		config = paxi.ConnectToMaster(*master, true, id)
		log.Infof("Received config %s\n", config)
	}

	d := new(db)
	d.c = paxi.NewClient(config)

	b := paxi.NewBenchmarker(d)
	b.Load()
	b.Run()
}
