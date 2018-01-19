package paxi

import "github.com/ailidani/paxi/log"

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
}

type socket struct {
	id    ID
	nodes map[ID]Transport
	codec Codec
}

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
