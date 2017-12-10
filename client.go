package paxi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"time"

	"github.com/ailidani/paxi/log"
)

// Client main access point of client lib
type Client struct {
	ID        ID // client id use the same size id as servers in local site
	N         int
	addrs     map[ID]string
	http      map[ID]string
	algorithm string

	index map[Key]ID
	cid   CommandID

	results map[CommandID]bool

	sync.RWMutex
	sync.WaitGroup
}

// NewClient creates a new Client from config
func NewClient(config *Config) *Client {
	c := new(Client)
	c.ID = config.ID
	c.N = len(config.Addrs)
	c.addrs = config.Addrs
	c.http = config.HTTPAddrs
	c.algorithm = config.Algorithm
	c.index = make(map[Key]ID)
	c.results = make(map[CommandID]bool, config.BufferSize)
	return c
}

func (c *Client) getNodeID(key Key) ID {
	c.RLock()
	defer c.RUnlock()
	id, exists := c.index[key]
	// if not exists, select first node in local site
	if !exists {
		id = NewID(c.ID.Zone(), 1)
	}
	return id
}

// RESTGet access server's REST API with url = http://ip:port/key
func (c *Client) RESTGet(key Key) Value {
	c.cid++
	id := c.getNodeID(key)
	url := c.http[id] + "/" + strconv.Itoa(int(key))

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	req.Header.Set("id", c.ID.String())
	req.Header.Set("cid", fmt.Sprintf("%v", c.cid))
	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer rep.Body.Close()
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugln(rep.Status)
	log.Debugf("%q", dump)
	if rep.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(rep.Body)
		return Value(b)
	}
	return nil
}

// RESTPut access server's REST API with url = http://ip:port/key and request body of value
func (c *Client) RESTPut(key Key, value Value) {
	c.cid++
	id := c.getNodeID(key)

	url := c.http[id] + "/" + strconv.Itoa(int(key))

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(value))
	if err != nil {
		log.Errorln(err)
		return
	}
	req.Header.Set("id", c.ID.String())
	req.Header.Set("cid", fmt.Sprintf("%v", c.cid))
	rep, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Errorln(err)
		return
	}
	defer rep.Body.Close()
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugln(rep.Status)
	log.Debugf("%q", dump)
}

// Get post json get request to server url
func (c *Client) Get(key Key) Value {
	return c.RESTGet(key)
}

// Put post json request
func (c *Client) Put(key Key, value Value) {
	c.RESTPut(key, value)
}

// GetAsync do Get request in goroutine
func (c *Client) GetAsync(key Key) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Get(key)
}

// PutAsync do Put request in goroutine
func (c *Client) PutAsync(key Key, value Value) {
	c.Add(1)
	c.Lock()
	c.results[c.cid+1] = false
	c.Unlock()
	go c.Put(key, value)
}

func (c *Client) JSONGet(key Key) Value {
	c.cid++
	cmd := Command{GET, key, nil}
	req := new(Request)
	req.ClientID = c.ID
	req.CommandID = c.cid
	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()

	id := c.getNodeID(key)

	url := c.http[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer rep.Body.Close()
	if rep.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(rep.Body)
		return Value(b)
	}
	return nil
}

func (c *Client) JSONPut(key Key, value Value) {
	c.cid++
	cmd := Command{PUT, key, value}
	req := new(Request)
	req.ClientID = c.ID
	req.CommandID = c.cid
	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()

	id := c.getNodeID(key)

	url := c.http[id]
	data, err := json.Marshal(*req)
	rep, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Errorln(err)
		return
	}
	defer rep.Body.Close()
	dump, _ := httputil.DumpResponse(rep, true)
	log.Debugln(rep.Status)
	log.Debugf("%q", dump)
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

func (c *Client) Start() {}

func (c *Client) Stop() {}
