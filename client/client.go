package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
)

var id = flag.String("id", "", "node id this client connects to")
var api = flag.String("api", "", "Client API type [rest, json, quorum]")
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
	key := paxi.Key(k)
	var v paxi.Value
	switch *api {
	case "rest":
		v = d.c.RESTGet(key)
	case "json":
		v = d.c.JSONGet(key)
	default:
		v = d.c.Get(key)
	}
	if len(v) == 0 {
		return 0
	}
	return int(binary.LittleEndian.Uint64(v))
}

func (d *db) Write(k, v int) {
	key := paxi.Key(k)
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, uint64(v))
	switch *api {
	case "rest":
		d.c.RESTPut(key, value)
	case "json":
		d.c.JSONPut(key, value)
	default:
		d.c.Put(paxi.Key(k), value)
	}
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	d := new(db)
	d.c = paxi.NewClient(paxi.ID(*id))

	b := paxi.NewBenchmark(d)
	b.Run()
}
