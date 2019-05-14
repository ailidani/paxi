package paxi

import (
	"time"

	"github.com/ailidani/paxi/log"
)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put message to outbound queue
	Send(to ID, m interface{})

	// MulticastZone send msg to all nodes in the same site
	MulticastZone(zone int, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	MulticastQuorum(quorum int, m interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(id ID, t int)             // drops every message send to ID last for t seconds
	Slow(id ID, d int, t int)      // delays every message send to ID for d ms and last for t seconds
	Flaky(id ID, p float32, t int) // drop message by chance p for t seconds
	Crash(t int)                   // node crash for t seconds
}

type socket struct {
	id    ID
	nodes map[ID]Transport

	crash bool
	drop  map[ID]bool
	slow  map[ID]int
	flaky map[ID]float32
}

// NewSocket return Socket interface instance given self ID, node list, transport and codec name
func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:    id,
		nodes: make(map[ID]Transport),
		drop:  make(map[ID]bool),
		slow:  make(map[ID]int),
		flaky: make(map[ID]float32),
	}

	socket.nodes[id] = NewTransport(addrs[id])
	socket.nodes[id].Listen()

	for id, addr := range addrs {
		if id == socket.id {
			continue
		}
		t := NewTransport(addr)
		err := Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err == nil {
			socket.nodes[id] = t
		} else {
			panic(err)
		}
	}
	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	if s.crash {
		return
	}
	// TODO also check for slow and flaky
	if s.drop[to] {
		return
	}
	t, exists := s.nodes[to]
	if !exists {
		log.Errorf("transport of ID %v does not exists", to)
		return
	}
	t.Send(m)
}

func (s *socket) Recv() interface{} {
	for {
		m := s.nodes[s.id].Recv()
		if !s.crash {
			return m
		}
	}
}

func (s *socket) MulticastZone(zone int, m interface{}) {
	log.Debugf("node %s broadcasting message %+v in zone %d", s.id, m, zone)
	for id := range s.nodes {
		if id == s.id {
			continue
		}
		if id.Zone() == zone {
			s.Send(id, m)
		}
	}
}

func (s *socket) MulticastQuorum(quorum int, m interface{}) {
	log.Debugf("node %s multicasting message %+v for %d nodes", s.id, m, quorum)
	i := 0
	for id := range s.nodes {
		if id == s.id {
			continue
		}
		s.Send(id, m)
		i++
		if i == quorum {
			break
		}
	}
}

func (s *socket) Broadcast(m interface{}) {
	log.Debugf("node %s broadcasting message %+v", s.id, m)
	for id := range s.nodes {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
}

func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

func (s *socket) Drop(id ID, t int) {
	s.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.drop[id] = false
	}()
}

func (s *socket) Slow(id ID, delay int, t int) {
	s.slow[id] = 0
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = delay
	}()
}

func (s *socket) Flaky(id ID, p float32, t int) {
	s.flaky[id] = 0
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = p
	}()
}

func (s *socket) Crash(t int) {
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
		}()
	}
}
