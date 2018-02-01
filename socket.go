package paxi

import (
	"time"

	"github.com/ailidani/paxi/log"
)

// Socket integrates all networking interface and fault injections
type Socket interface {

	// Send put msg to outbound queue
	Send(to ID, msg interface{})

	// Multicast send msg to all nodes in the same site
	Multicast(zone int, msg interface{})

	// Broadcast send to all peers
	Broadcast(msg interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(ID, int)  // drops every message send to ID for t seconds
	Slow(ID, int)  // delays every message send to ID for t seconds
	Flaky(ID, int) // drop message by chance for t seconds
	Crash(int)     // node crash for t seconds
}

type socket struct {
	id    ID
	nodes map[ID]Transport
	codec Codec

	drop  map[ID]bool
	slow  map[ID]bool
	flaky map[ID]bool
}

// NewSocket return Socket interface instance given self ID, node list, transport and codec name
func NewSocket(id ID, addrs map[ID]string, transport, codec string) Socket {
	socket := new(socket)
	socket.id = id
	socket.nodes = make(map[ID]Transport)
	socket.codec = NewCodec(codec)

	socket.nodes[id] = NewTransport(transport + "://" + addrs[id])
	go socket.nodes[id].Listen()

	for id, addr := range addrs {
		if id == socket.id {
			continue
		}
		t := NewTransport(transport + "://" + addr)
		err := t.Dial()
		for err != nil {
			err = t.Dial()
		}
		socket.nodes[id] = t
	}
	return socket
}

func (s *socket) Send(to ID, msg interface{}) {
	// TODO also check for slow and flaky
	if s.drop[to] {
		return
	}
	t, ok := s.nodes[to]
	if !ok {
		log.Fatalf("transport of ID %v does not exists", to)
	}
	b := s.codec.Encode(msg)
	m := NewMessage(len(b))
	m.Body = b
	t.Send(m)
}

func (s *socket) Recv() interface{} {
	m := s.nodes[s.id].Recv()
	msg := s.codec.Decode(m.Body)
	return msg
}

func (s *socket) Multicast(zone int, msg interface{}) {
	for id := range s.nodes {
		if id == s.id {
			continue
		}
		if id.Zone() == zone {
			s.Send(id, msg)
		}
	}
}

func (s *socket) Broadcast(msg interface{}) {
	for id := range s.nodes {
		if id == s.id {
			continue
		}
		s.Send(id, msg)
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

func (s *socket) Slow(id ID, t int) {
	s.slow[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = false
	}()
}

func (s *socket) Flaky(id ID, t int) {
	s.flaky[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = false
	}()
}

func (s *socket) Crash(t int) {
	// TODO not implemented
	log.Fatal("Crash function not implemented")
}
