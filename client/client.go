package main

import (
	"encoding/binary"
	"flag"

	"github.com/ailidani/paxi"
)

var id = flag.String("id", "", "node id this client connects to")
var api = flag.String("api", "", "Client API type [rest, json, quorum]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	c *paxi.Client
}

func (d *db) Init() error {
	d.c.Start()
	return nil
}

func (d *db) Stop() error {
	d.c.Stop()
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := paxi.Key(k)
	var v paxi.Value
	var err error
	switch *api {
	case "rest":
		v, err = d.c.RESTGet(key)
	case "json":
		v, err = d.c.JSONGet(key)
	default:
		v, err = d.c.Get(key)
	}
	if len(v) == 0 {
		return 0, nil
	}
	x, _ := binary.Uvarint(v)
	return int(x), err
}

func (d *db) Write(k, v int) error {
	key := paxi.Key(k)
	value := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(value, uint64(v))
	var err error
	switch *api {
	case "rest":
		_, err = d.c.RESTPut(key, value)
	case "json":
		_, err = d.c.JSONPut(key, value)
	default:
		_, err = d.c.Put(paxi.Key(k), value)
	}
	return err
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	d := new(db)
	d.c = paxi.NewClient(paxi.ID(*id))

	b := paxi.NewBenchmark(d)
	if *load {
		b.Load()
	} else {
		b.Run()
	}
}
