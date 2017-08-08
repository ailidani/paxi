package main

import (
	"encoding/gob"
	"flag"
	"log"
	"net"
	"paxi"
	"strconv"
	"strings"
)

var n = flag.Int("n", 1, "N number of replicas, default value 1.")
var protocol = flag.String("protocol", "wpaxos", "Consensus protocol name")
var consistency = flag.Int("c", 1, "Consistency level")
var F = flag.Int("f", 0, "failure per site")
var Threshold = flag.Int("threshold", 0, "Threshold for leader change, 0 means immediate")
var BackOff = flag.Int("backoff", 100, "Random backoff time")
var Thrifty = flag.Bool("thrifty", false, "")

var ChanBufferSize = flag.Int("chanbufsize", paxi.CHAN_BUFFER_SIZE, "")
var BufferSize = flag.Int("bufsize", paxi.BUFFER_SIZE, "")

func main() {
	flag.Parse()

	log.Println("Master server starting...")

	gob.Register(paxi.Register{})
	gob.Register(paxi.Config{})

	in := make(chan paxi.Register)
	out := make(chan paxi.Config)

	var p paxi.Protocol
	switch *protocol {
	case "wpaxos":
		p = paxi.WPaxos
	case "epaxos":
		p = paxi.EPaxos
	case "kpaxos":
		p = paxi.KPaxos
	}

	config := paxi.Config{
		Protocol:       p,
		F:              *F,
		Threshold:      *Threshold,
		BackOff:        *BackOff,
		Thrifty:        *Thrifty,
		ChanBufferSize: *ChanBufferSize,
		BufferSize:     *BufferSize,
	}

	go func() {
		nodes := make(map[paxi.ID]string, *n)
		for i := 0; i < *n; i++ {
			msg := <-in
			id := msg.ID
			nodes[id] = msg.Addr + ":" + strconv.Itoa(paxi.PORT+i+1)
			log.Printf("Node %v address %s\n", id, nodes[id])
		}
		config.Addrs = nodes
		for i := 0; i < *n; i++ {
			out <- config
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
			switch msg.EndpointType {
			case paxi.NODE:
				msg.Addr = strings.Split(conn.RemoteAddr().String(), ":")[0]
				log.Printf("Node %s address %s\n", msg.ID, msg.Addr)
				in <- msg
				c = <-out
			case paxi.CLIENT:
				c = config
			default:
				log.Panicf("Node %s unknown type\n", msg.ID)
			}
			c.ID = msg.ID
			log.Printf("Sending Config %v\n", c)
			err = encoder.Encode(c)
			if err != nil {
				log.Panicln(err)
			}
		}(conn)
	}

	log.Println("Master done.")
}
