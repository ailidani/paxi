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
var paxosQuorumRead = flag.Bool("pqr", false, "Use Paxos Quourm Reads")

// db implements Paxi.DB interface for benchmarking
type db struct {
	paxi.Client
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := paxi.Key(k)
	v, err := d.Get(key)
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
	err := d.Put(key, value)
	return err
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	d := new(db)
	d.Client = paxi.NewHTTPClient(paxi.ID(*id), *paxosQuorumRead)

	b := paxi.NewBenchmark(d)
	if *load {
		b.Load()
	} else {
		b.Run()
	}
}
