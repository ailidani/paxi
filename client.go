package paxi

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"paxi/glog"
	"sync"
	"time"
)

// Client main access point of client lib
type Client struct {
	ID    ID
	N     int
	addrs map[ID]string

	index map[Key]ID
	cid   CommandID

	results map[CommandID]bool

	sync.RWMutex
	sync.WaitGroup
}

// NewClient creates a new Client from config
func NewClient(config *Config) *Client {
	gob.Register(Request{})
	gob.Register(Reply{})

	c := new(Client)
	c.ID = config.ID
	c.N = len(config.Addrs)
	c.addrs = config.HTTPAddrs
	c.index = make(map[Key]ID)
	c.results = make(map[CommandID]bool, config.BufferSize)
	return c
}

// Get post json get request to server url
func (c *Client) Get(key Key) Value {
	c.cid++
	cmd := Command{GET, key, nil}
	req := new(Request)
	req.ClientID = c.ID
	req.CommandID = c.cid
	req.Commands = []Command{cmd}
	req.Timestamp = time.Now().UnixNano()

	c.RLock()
	id, exists := c.index[key]
	c.RUnlock()
	if !exists {
		id = NewID(c.ID.Site(), 1)
	}

	url := c.addrs[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		glog.Errorln(err)
		return nil
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(rep.Body)
		return Value(b)
	}
	return nil
}

// GetAsync do Get request in goroutine
func (c *Client) GetAsync(key Key) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Get(key)
}

// Put post json request
func (c *Client) Put(key Key, value Value) {
	c.cid++
	cmd := Command{PUT, key, value}
	req := new(Request)
	req.ClientID = c.ID
	req.CommandID = c.cid
	req.Commands = []Command{cmd}
	req.Timestamp = time.Now().UnixNano()

	c.RLock()
	id, exists := c.index[key]
	c.RUnlock()
	if !exists {
		id = NewID(c.ID.Site(), 1)
	}

	url := c.addrs[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		glog.Errorln(err)
		return
	}
	defer rep.Body.Close()
	dump, _ := httputil.DumpResponse(rep, true)
	log.Println(rep.Status)
	log.Printf("%q", dump)
}

// PutAsync do Put request in goroutine
func (c *Client) PutAsync(key Key, value Value) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Put(key, value)
}

// RequestDone returns the total number of succeed async reqeusts
func (c *Client) RequestDone() int {
	sum := 0
	for _, succeed := range c.results {
		if succeed {
			sum++
		}
	}
	return sum
}

func (c *Client) Start() {
	// for id, url := range c.addrs {
	// 	rep, err := http.Get(url + "/100")
	// 	if err != nil {
	// 		glog.Fatalln(err)
	// 	}
	// 	if rep.StatusCode == http.StatusOK {
	// 		glog.Infoln("http get to node ", id)
	// 	} else {
	// 		glog.Errorln(rep.Status)
	// 	}
	// }
}

func (c *Client) Stop() {}
