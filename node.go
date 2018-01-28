package paxi

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"

	"github.com/ailidani/paxi/log"
)

// TODO these global states are used by Quorums, too magical
var (
	// NumZones total number of sites
	NumZones int
	// NumNodes total number of nodes
	NumNodes int
	// NumLocalNodes number of nodes per site
	NumLocalNodes int
	// F number of zone failures
	F int
	// QuorumType name of the quorums
	QuorumType string
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type Node interface {
	Socket
	Database
	ID() ID
	Config() Config
	Run()
	Retry(r Request)
	Forward(id ID, r Request)
	Register(m interface{}, f interface{})
}

// node implements Node interface
type node struct {
	id     ID
	config Config

	Socket
	Database
	MessageChan chan interface{}
	handles     map[string]reflect.Value
}

// NewNode creates a new Node object from configuration
func NewNode(config Config) Node {
	node := new(node)
	node.id = config.ID
	node.config = config

	node.Socket = NewSocket(config.ID, config.Addrs, config.Transport, config.Codec)
	node.Database = NewDatabase()
	node.MessageChan = make(chan interface{}, config.ChanBufferSize)
	node.handles = make(map[string]reflect.Value)

	zones := make(map[int]int)
	for id := range config.Addrs {
		zones[id.Zone()]++
	}
	NumZones = len(zones)
	NumNodes = len(config.Addrs)
	NumLocalNodes = zones[config.ID.Zone()]
	F = config.F
	QuorumType = config.Quorum

	return node
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Config() Config {
	return n.config
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
	log.Infof("node %v start running\n", n.id)
	if len(n.handles) > 0 {
		go n.handle()
		go n.recv()
	}
	n.http()
}

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/history/", n.handleHistory)
	// http string should be in form of ":8080"
	url, err := url.Parse(n.config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	log.Fatal(http.ListenAndServe(port, mux))
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
	url := n.config.HTTPAddrs[id] + "/" + strconv.Itoa(int(key))

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
	req.Header.Set(HttpClientID, string(m.Command.ClientID))
	req.Header.Set(HttpCommandID, strconv.Itoa(m.Command.CommandID))
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
