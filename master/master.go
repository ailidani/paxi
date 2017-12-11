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

var n = flag.Int("n", 1, "N number of replicas, default value 1.")
var algorithm = flag.String("algorithm", "cosmos", "Consensus algorithm name")
var consistency = flag.Int("c", 1, "Consistency level")
var f = flag.Int("f", 0, "failure per site")
var threshold = flag.Int("threshold", 0, "Threshold for leader change, 0 means immediate")
var backOff = flag.Int("backoff", 100, "Random backoff time")
var thrifty = flag.Bool("thrifty", false, "")
var transport = flag.String("transport", "udp", "Transport protocols, including tcp, udp, chan (local)")
var replywhencommit = flag.Bool("replywhencommit", false, "reply to client when request is committed, not executed")

var chanbufsize = flag.Int("chanbufsize", paxi.CHAN_BUFFER_SIZE, "")
var bufsize = flag.Int("bufsize", paxi.BUFFER_SIZE, "")

func main() {
	flag.Parse()

	log.Println("Master server starting...")

	in := make(chan paxi.Register)
	out := make(chan paxi.Config)

	config := paxi.MakeDefaultConfig()
	config.Algorithm = *algorithm
	config.F = *f
	config.Threshold = *threshold
	config.BackOff = *backOff
	config.Thrifty = *thrifty
	config.ChanBufferSize = *chanbufsize
	config.BufferSize = *bufsize

	go func() {
		addrs := make(map[paxi.ID]string, *n)
		http := make(map[paxi.ID]string, *n)
		for i := 0; i < *n; i++ {
			msg := <-in
			id := msg.ID
			addrs[id] = *transport + "://" + msg.Addr + ":" + strconv.Itoa(paxi.PORT+i+1)
			http[id] = "http://" + msg.Addr + ":" + strconv.Itoa(paxi.HTTP_PORT+i+1)
			log.Printf("Node %v address %s\n", id, addrs[id])
		}
		config.Addrs = addrs
		config.HTTPAddrs = http
		for i := 0; i < *n; i++ {
			out <- *config
		}
	}()

	listener, err := net.Listen("tcp", ":"+strconv.Itoa(paxi.PORT))
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
				c = *config
			}
			c.ID = msg.ID
			err = encoder.Encode(c)
			if err != nil {
				log.Panicln(err)
			}
		}(conn)
	}
}
