package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"

	. "github.com/ailidani/paxi"
)

var sid = flag.Int("sid", 1, "Site ID.")
var nid = flag.Int("nid", 1, "Node ID.")
var master = flag.String("master", "127.0.0.1", "Master address.")

func usage() string {
	return fmt.Sprint("cmd {get key | put key value}")
}

func main() {
	flag.Parse()
	config := ConnectToMaster(*master, true, NewID(uint8(*sid), uint8(*nid)))
	client := NewClient(config)
	client.Start()

	if len(os.Args) < 2 {
		log.Println(usage())
		return
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "get":
		k, _ := strconv.Atoi(args[0])
		v := client.Get(Key(k))
		log.Println(v)
	case "put":
		k, _ := strconv.Atoi(args[0])
		client.Put(Key(k), []byte(args[1]))
	default:
		log.Println(usage())
	}
}
