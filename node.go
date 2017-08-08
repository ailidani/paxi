package paxi

import (
	"encoding/gob"
	"net"
	"paxi/glog"
	"strings"
	"sync"
)

var (
	NumSites      int
	NumNodes      int
	NumLocalNodes int
	Q1Size        int
	Q2Size        int
)

type transport struct {
	addr    string
	conn    net.Conn
	encoder *gob.Encoder
	decoder *gob.Decoder
}

type Node struct {
	ID      ID
	Peers   map[ID]*transport
	Clients map[ID]*transport

	State       *StateMachine
	RequestChan chan Request
	ReplyChan   chan Reply
	MessageChan chan Message

	sync.RWMutex

	Threshold int
	BackOff   int  // random backoff interval
	Thrifty   bool // only send messages to a quorum
}

func NewNode(config *Config) *Node {
	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(Register{})

	n := &Node{
		ID:          config.ID,
		Peers:       make(map[ID]*transport),
		Clients:     make(map[ID]*transport),
		State:       NewStateMachine(),
		RequestChan: make(chan Request, config.ChanBufferSize),
		ReplyChan:   make(chan Reply, config.ChanBufferSize),
		MessageChan: make(chan Message, config.ChanBufferSize),
		Threshold:   config.Threshold,
		BackOff:     config.BackOff,
		Thrifty:     config.Thrifty,
	}

	sites := make(map[uint8]int)
	for id, addr := range config.Addrs {
		n.Peers[id] = &transport{addr: addr}
		sites[id.Site()]++
	}
	NumSites = len(sites)
	NumNodes = len(n.Peers)
	NumLocalNodes = sites[n.ID.Site()]
	Q1Size = NumSites * (config.F + 1)
	Q2Size = NumLocalNodes - config.F

	return n
}

func (n *Node) Run() {
	go n.replying()
	go n.connecting()
	n.listening()
}

// replying to clients
func (n *Node) replying() {
	for {
		reply := <-n.ReplyChan
		glog.V(2).Infof("Replica %s sending %v\n", n.ID, reply)
		n.RLock()
		err := n.Clients[reply.ClientID].encoder.Encode(reply)
		n.RUnlock()
		if err != nil {
			glog.Errorln("Replica send reply error", err)
		}
	}
}

func (n *Node) listening() {
	addr := n.Peers[n.ID].addr
	port := strings.Split(addr, ":")[1]
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		glog.Fatalln("Listener error: ", err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			glog.Errorln("Accept error: ", err)
			continue
		}
		go func(conn net.Conn) {
			decoder := gob.NewDecoder(conn)
			encoder := gob.NewEncoder(conn)
			var msg Message
			for {
				err := decoder.Decode(&msg)
				if err != nil {
					glog.Errorln(err)
					conn.Close()
					break
				}
				switch msg := msg.(type) {
				case Register:
					if msg.EndpointType == CLIENT {
						n.Lock()
						n.Clients[msg.ID] = &transport{
							addr:    conn.RemoteAddr().String(),
							conn:    conn,
							encoder: encoder,
							decoder: decoder,
						}
						n.Unlock()
					}

				case Request:
					n.RequestChan <- msg

				case Reply:
					// TODO
					glog.Fatalln("Forward and reply Not implemented.")

				default:
					n.MessageChan <- msg
				}
			}
		}(conn)
	}
}

func (n *Node) connecting() {
	var err error
	done := len(n.Peers) - 1
	for done > 0 {
		for id, peer := range n.Peers {
			if id == n.ID {
				continue
			}
			if peer.conn != nil {
				continue
			}
			peer.conn, err = net.Dial("tcp", peer.addr)
			if err != nil {
				continue
			}
			peer.decoder = gob.NewDecoder(peer.conn)
			peer.encoder = gob.NewEncoder(peer.conn)
			// todo listen??
			done--
		}
	}
}

func (n *Node) Broadcast(msg Message) {
	for id, peer := range n.Peers {
		if id == n.ID {
			continue
		}
		err := peer.encoder.Encode(&msg)
		if err != nil {
			glog.Errorln("Node broadcast error", err)
		}
	}
}

func (n *Node) Multicast(msg Message) {
	for id, peer := range n.Peers {
		if id == n.ID {
			continue
		}
		if id.Site() == n.ID.Site() {
			err := peer.encoder.Encode(&msg)
			if err != nil {
				glog.Errorln("Node multicast error", err)
			}
		}
	}
}

func (n *Node) Send(to ID, msg Message) {
	if n.Peers[to].encoder == nil {
		return
	}
	err := n.Peers[to].encoder.Encode(&msg)
	if err != nil {
		glog.Errorln("Node reply error", err)
	}
}

func (n *Node) Forward(to ID, request Request) {
	n.Send(to, request)
	// TODO wait for reply??
}
