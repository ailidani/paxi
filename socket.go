package paxi

type Socket interface {

	// Send put msg to outbound queue
	Send(to ID, msg interface{})

	// Multicast send msg to all nodes in the same site
	Multicast(sid uint8, msg interface{})

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

func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := new(socket)
	socket.id = id
	socket.nodes = make(map[ID]Transport)
	socket.codec = NewCodec("gob")

	socket.nodes[id] = NewTransport(addrs[id])
	go socket.nodes[id].Listen()

	done := len(addrs) - 1
	for done > 0 {
		for id, addr := range addrs {
			if id == socket.id {
				continue
			}
			socket.nodes[id] = NewTransport(addr)
			err := socket.nodes[id].Dial()
			if err != nil {
				continue
			}
			done--

		}
	}
	return socket
}

func (sock *socket) Send(to ID, msg interface{}) {
	b := sock.codec.Encode(msg)
	m := NewMessage(len(b))
	m.Body = b
	sock.nodes[to].Send(m)
}

func (sock *socket) Recv() interface{} {
	m := sock.nodes[sock.id].Recv()
	msg := sock.codec.Decode(m.Body)
	return msg
}

func (sock *socket) Multicast(sid uint8, msg interface{}) {
	for id := range sock.nodes {
		if id == sock.id {
			continue
		}
		if id.Site() == sid {
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
