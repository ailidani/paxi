package main

import (
	"flag"
	"log"
	"os"
	. "paxi"
	"strconv"
)

var sid = flag.Int("sid", 1, "Site ID.")
var nid = flag.Int("nid", 1, "Node ID.")
var addr = flag.String("master", "127.0.0.1", "Master address.")

func main() {
	flag.Parse()
	config := ConnectToMaster(*addr, true, NewID(uint8(*sid), uint8(*nid)))
	client := NewClient(config)
	client.Start()

	if len(os.Args) < 2 {
		log.Println("cmd {get key | put key value}")
		return
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "get":
		k, _ := strconv.Atoi(args[0])
		client.Get(Key(k))
	case "put":
		k, _ := strconv.Atoi(args[0])
		client.Put(Key(k), []byte(args[1]))
	default:
		log.Println("cmd {get key | put key value}")
	}
}
