package paxi

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"

	"github.com/ailidani/paxi/log"
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	Socket
	Database
	ID() ID
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
}

// node implements Node interface
type node struct {
	id ID

	Socket
	Database
	MessageChan chan interface{}
	handles     map[string]reflect.Value
	server      *http.Server
}

// NewNode creates a new Node object from configuration
func NewNode(id ID) Node {
	return &node{
		id:          id,
		Socket:      NewSocket(id, Config.Addrs, Config.Transport),
		Database:    NewDatabase(),
		MessageChan: make(chan interface{}, Config.ChanBufferSize),
		handles:     make(map[string]reflect.Value),
	}
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Retry(r Request) {
	n.MessageChan <- r
}

// Register a handle function for each message type
func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

// Run start and run the node
func (n *node) Run() {
	log.Infof("node %v start running", n.id)
	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
	}
	n.http()
}

// recv receives messages from socket and pass to message channel
func (n *node) recv() {
	for {
		n.MessageChan <- n.Recv()
	}
}

// handle receives messages from message channel and calls handle function using refection
func (n *node) handle() {
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

func (n *node) Forward(id ID, m Request) {
	key := m.Command.Key
	url := Config.HTTPAddrs[id] + "/" + strconv.Itoa(int(key))

	log.Debugf("Node %v forwarding request %v to %s", n.ID(), m, url)

	method := http.MethodGet
	var body io.Reader
	if !m.Command.IsRead() {
		method = http.MethodPut
		body = bytes.NewBuffer(m.Command.Value)
	}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return
	}
	req.Header.Set(HTTPClientID, string(n.id))
	req.Header.Set(HTTPCommandID, strconv.Itoa(m.Command.CommandID))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Error(err)
		m.Reply(Reply{
			Command: m.Command,
			Err:     err,
		})
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		m.Reply(Reply{
			Command: m.Command,
			Value:   Value(b),
		})
	} else {
		m.Reply(Reply{
			Command: m.Command,
			Err:     errors.New(res.Status),
		})
	}
}
