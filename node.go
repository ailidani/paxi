package paxi

import (
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"paxi/glog"
	"strconv"
	"sync"
)

var (
	NumSites      int
	NumNodes      int
	NumLocalNodes int
	Q1Size        int
	Q2Size        int
)

// Node is the base class for every replica
// it includes networking, state machine and client handshake logic
type Node struct {
	ID ID
	Socket
	http string

	State       *StateMachine
	RequestChan chan Request
	MessageChan chan interface{}

	sync.RWMutex

	BackOff int  // random backoff interval
	Thrifty bool // only send messages to a quorum
}

// NewNode creates a new Node object from configuration
func NewNode(config *Config) *Node {
	gob.Register(Request{})
	gob.Register(Reply{})

	node := new(Node)
	node.ID = config.ID
	node.Socket = NewSocket(config.ID, config.Addrs)
	url, _ := url.Parse(config.HTTPAddrs[config.ID])
	node.http = ":" + url.Port()
	node.State = NewStateMachine()
	node.RequestChan = make(chan Request, config.ChanBufferSize)
	node.MessageChan = make(chan interface{}, config.ChanBufferSize)
	node.BackOff = config.BackOff
	node.Thrifty = config.Thrifty

	sites := make(map[uint8]int)
	for id := range config.Addrs {
		sites[id.Site()]++
	}
	NumSites = len(sites)
	NumNodes = len(config.Addrs)
	NumLocalNodes = sites[config.ID.Site()]
	Q1Size = NumSites * (config.F + 1)
	Q2Size = NumLocalNodes - config.F

	return node
}

func (n *Node) Run() {
	go n.recv()
	n.serve()
}

func (n *Node) serve() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		req.w = w
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			glog.Errorln("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}

		if len(r.URL.Path) > 1 {
			i, _ := strconv.Atoi(r.URL.Path[1:])
			key := Key(i)
			switch r.Method {
			case http.MethodGet:
				req.Commands = []Command{Command{GET, key, nil}}
			case http.MethodPut:
			case http.MethodPost:
				req.Commands = []Command{Command{PUT, key, body}}
			}
		} else {
			json.Unmarshal(body, &req)
		}

		n.RequestChan <- req
	})
	err := http.ListenAndServe(n.http, nil)
	if err != nil {
		glog.Fatalln(err)
	}
}

func (n *Node) recv() {
	for {
		msg := n.Recv()
		glog.Infoln("Received msg ", msg)
		n.MessageChan <- msg
	}
}
