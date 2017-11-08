package paxi

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"paxi/glog"
	"reflect"
	"strconv"
	"sync"
)

var (
	// NumSites total number of sites
	NumSites int
	// NumNodes total number of nodes
	NumNodes int
	// NumLocalNodes number of nodes per site
	NumLocalNodes int
	// Q1Size phase one quorum size
	Q1Size int
	// Q2Size phase two quorum size
	Q2Size int
)

// Node is the base class for every replica
// it includes networking, state machine and client handshake logic
type Node struct {
	ID ID
	Socket
	http string

	DB          *StateMachine
	MessageChan chan interface{}
	handles     map[string]reflect.Value

	sync.RWMutex

	BackOff int  // random backoff interval
	Thrifty bool // only send messages to a quorum
}

// NewNode creates a new Node object from configuration
func NewNode(config *Config) *Node {
	node := new(Node)
	node.ID = config.ID
	node.Socket = NewSocket(config.ID, config.Addrs)
	url, _ := url.Parse(config.HTTPAddrs[config.ID])
	node.http = ":" + url.Port()
	node.DB = NewStateMachine()
	node.MessageChan = make(chan interface{}, config.ChanBufferSize)
	node.handles = make(map[string]reflect.Value)
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

// Register a handle function for each message type
func (n *Node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *Node) Run() {
	glog.Infof("node %v start running\n", n.ID)
	if len(n.handles) > 0 {
		go n.handle()
	}
	go n.recv()
	n.serve()
}

func (n *Node) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		req.w = w
		//req.ClientID, _ = IDFromString(r.Header.Get("id"))
		cid, _ := strconv.Atoi(r.Header.Get("cid"))
		req.CommandID = CommandID(cid)

		if len(r.URL.Path) > 1 {
			i, _ := strconv.Atoi(r.URL.Path[1:])
			key := Key(i)
			switch r.Method {
			case http.MethodGet:
				req.Command = Command{GET, key, nil}
			case http.MethodPut, http.MethodPost:
				body, err := ioutil.ReadAll(r.Body)
				if err != nil {
					glog.Errorln("error reading body: ", err)
					http.Error(w, "cannot read body", http.StatusBadRequest)
					return
				}
				req.Command = Command{PUT, key, Value(body)}
			}
		} else {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				glog.Errorln("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			json.Unmarshal(body, &req)
		}

		n.MessageChan <- req
	})
	err := http.ListenAndServe(n.http, mux)
	if err != nil {
		glog.Fatalln(err)
	}
}

func (n *Node) recv() {
	for {
		n.MessageChan <- n.Recv()
	}
}

func (n *Node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			glog.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v})
	}
}
