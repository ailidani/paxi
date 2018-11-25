package main

import (
	"encoding/gob"
	"flag"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/ailidani/paxi"
)

var port = flag.Int("port", 1735, "master port number")
var httpPort = flag.Int("http", 8080, "http port")

var n = flag.Int("n", 1, "N number of replicas, default value 1.")
var threshold = flag.Float64("threshold", 3.0, "Threshold for leader change")
var thrifty = flag.Bool("thrifty", false, "")
var transport = flag.String("transport", "tcp", "Transport protocols, including tcp, udp, chan (local)")

func main() {
	flag.Parse()

	log.Println("Master server starting...")

	in := make(chan paxi.Register)
	out := make(chan paxi.Config)

	config := paxi.MakeDefaultConfig()
	config.Threshold = *threshold
	config.Thrifty = *thrifty

	go func() {
		addrs := make(map[paxi.ID]string, *n)
		http := make(map[paxi.ID]string, *n)
		for i := 0; i < *n; i++ {
			msg := <-in
			id := msg.ID
			addrs[id] = msg.Addr + ":" + strconv.Itoa(*port+i+1)
			http[id] = "http://" + msg.Addr + ":" + strconv.Itoa(*httpPort+i+1)
			log.Printf("Node %v address %s\n", id, addrs[id])
		}
		config.Addrs = addrs
		config.HTTPAddrs = http
		for i := 0; i < *n; i++ {
			out <- config
		}
	}()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(*port))
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Waiting for connection...")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Panicln(err)
			continue
		}
		log.Printf("Connected with %s\n", conn.RemoteAddr())
		go func(conn net.Conn) {
			decoder := gob.NewDecoder(conn)
			encoder := gob.NewEncoder(conn)
			var msg paxi.Register
			err := decoder.Decode(&msg)
			if err != nil {
				log.Panicln(err)
				conn.Close()
				return
			}
			var c paxi.Config
			if !msg.Client {
				msg.Addr = strings.Split(conn.RemoteAddr().String(), ":")[0]
				log.Printf("Node %s address %s\n", msg.ID, msg.Addr)
				in <- msg
				c = <-out
			} else {
				c = config
			}
			err = encoder.Encode(c)
			if err != nil {
				log.Panicln(err)
			}
		}(conn)
	}
}
