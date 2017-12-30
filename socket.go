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

func (sock *socket) Send(to ID, msg interface{}) {
	t, ok := sock.nodes[to]
	if !ok {
		log.Fatalf("transport of ID %v does not exists", to)
	}
	b := sock.codec.Encode(msg)
	m := NewMessage(len(b))
	m.Body = b
	t.Send(m)
}

func (sock *socket) Recv() interface{} {
	m := sock.nodes[sock.id].Recv()
	msg := sock.codec.Decode(m.Body)
	return msg
}

func (sock *socket) Multicast(zone int, msg interface{}) {
	for id := range sock.nodes {
		if id == sock.id {
			continue
		}
		if id.Zone() == zone {
			sock.Send(id, msg)
		}
	}
}

func (sock *socket) Broadcast(msg interface{}) {
	for id := range sock.nodes {
		if id == sock.id {
			continue
		}
		sock.Send(id, msg)
	}
}

func (sock *socket) Close() {
	for _, t := range sock.nodes {
		t.Close()
	}
}
