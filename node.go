package paxi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"sync"

	"github.com/ailidani/paxi/log"
)

var (
	// NumZones total number of sites
	NumZones int
	// NumNodes total number of nodes
	NumNodes int
	// NumLocalNodes number of nodes per site
	NumLocalNodes int
	// F number of zone failures
	F int
)

// Node is the base class for every replica
// it includes networking, state machine and client handshake logic
type Node struct {
	ID     ID
	http   string
	Config Config

	Socket
	DB          *StateMachine
	MessageChan chan interface{}
	handles     map[string]reflect.Value

	sync.RWMutex
}

// NewNode creates a new Node object from configuration
func NewNode(config Config) *Node {
	node := new(Node)
	node.ID = config.ID
	node.Config = config

	// http string should be in form of ":8080"
	url, _ := url.Parse(config.HTTPAddrs[config.ID])
	node.http = ":" + url.Port()

	node.Socket = NewSocket(config.ID, config.Addrs, config.Codec)
	node.DB = NewStateMachine()
	node.MessageChan = make(chan interface{}, config.ChanBufferSize)
	node.handles = make(map[string]reflect.Value)

	zones := make(map[uint8]int)
	for id := range config.Addrs {
		zones[id.Zone()]++
	}
	NumZones = len(zones)
	NumNodes = len(config.Addrs)
	NumLocalNodes = zones[config.ID.Zone()]
	F = config.F

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
	log.Infof("node %v start running\n", n.ID)
	if len(n.handles) > 0 {
		go n.handle()
	}
	go n.recv()
	n.serve()
}

// serve serves the http REST API request from clients
func (n *Node) serve() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		req.c = make(chan Reply)
		req.ClientID, _ = IDFromString(r.Header.Get("id"))
		cid, _ := strconv.Atoi(r.Header.Get("cid"))
		req.CommandID = CommandID(cid)
		req.Timestamp, _ = strconv.ParseInt(r.Header.Get("timestamp"), 10, 64)

		if len(r.URL.Path) > 1 {
			i, _ := strconv.Atoi(r.URL.Path[1:])
			key := Key(i)
			switch r.Method {
			case http.MethodGet:
				req.Command = Command{GET, key, nil}
			case http.MethodPut, http.MethodPost:
				body, err := ioutil.ReadAll(r.Body)
				if err != nil {
					log.Errorln("error reading body: ", err)
					http.Error(w, "cannot read body", http.StatusBadRequest)
					return
				}
				req.Command = Command{PUT, key, Value(body)}
			}
		} else {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorln("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			json.Unmarshal(body, &req)
		}

		n.MessageChan <- req

		reply := <-req.c
		if reply.Err != nil {
			http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
			return
		}
		// r.w.Header().Set("ok", fmt.Sprintf("%v", reply.OK))
		w.Header().Set("id", reply.ClientID.String())
		w.Header().Set("cid", fmt.Sprintf("%v", reply.CommandID))
		w.Header().Set("timestamp", fmt.Sprintf("%v", reply.Timestamp))
		if reply.Command.IsRead() {
			_, err := io.WriteString(w, string(reply.Command.Value))
			// _, err := r.w.Write(reply.Command.Value)
			if err != nil {
				log.Errorln(err)
			}
		}
	})
	err := http.ListenAndServe(n.http, mux)
	if err != nil {
		log.Fatalln(err)
	}
}

// recv receives messages from socket and pass to message channel
func (n *Node) recv() {
	for {
		n.MessageChan <- n.Recv()
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *Node) handle() {
	for {
		msg := <-n.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		f.Call([]reflect.Value{v})
	}
}

func (n *Node) Forward(id ID, m Request) {
	key := m.Command.Key
	url := n.Config.HTTPAddrs[id] + "/" + strconv.Itoa(int(key))

	if m.Command.IsRead() {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			log.Errorln(err)
			return
		}
		req.Header.Set("id", m.ClientID.String())
		rep, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Errorln(err)
		}
		defer rep.Body.Close()
		if rep.StatusCode == http.StatusOK {
			b, _ := ioutil.ReadAll(rep.Body)
			cmd := m.Command
			cmd.Value = Value(b)
			m.Reply(Reply{
				OK:        true,
				ClientID:  m.ClientID,
				CommandID: m.CommandID,
				Command:   cmd,
			})
		}
	} else {
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(m.Command.Value))
		if err != nil {
			log.Errorln(err)
			return
		}
		req.Header.Set("id", m.ClientID.String())
		rep, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Errorln(err)
			return
		}
		defer rep.Body.Close()
		if rep.StatusCode == http.StatusOK {
			m.Reply(Reply{
				OK:        true,
				ClientID:  m.ClientID,
				CommandID: m.CommandID,
			})
		}
	}
}
