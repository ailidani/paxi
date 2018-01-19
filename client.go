package paxi

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"time"

	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
)

// Client main access point of client lib
type Client struct {
	ID        ID // client id use the same id as servers in local site
	N         int
	addrs     map[ID]string
	http      map[ID]string
	algorithm string

	cid CommandID
}

// NewClient creates a new Client from config
func NewClient(config Config) *Client {
	return &Client{
		ID:        config.ID,
		N:         len(config.Addrs),
		addrs:     config.Addrs,
		http:      config.HTTPAddrs,
		algorithm: config.Algorithm,
	}
}

// rest accesses server's REST API with url = http://ip:port/key
// if value == nil, it's read
func (c *Client) rest(id ID, key Key, value Value) Value {
	url := c.http[id] + "/" + strconv.Itoa(int(key))
	r := new(http.Request)
	var err error
	var o string
	if value == nil {
		r, err = http.NewRequest(http.MethodGet, url, nil)
		o = "get"
	} else {
		r, err = http.NewRequest(http.MethodPut, url, bytes.NewBuffer(value))
		o = "put"
	}
	if err != nil {
		log.Errorln(err)
		return nil
	}
	r.Header.Set("id", string(c.ID))
	r.Header.Set("cid", strconv.FormatUint(uint64(c.cid), 10))
	r.Header.Set("timestamp", strconv.FormatInt(time.Now().UnixNano(), 10))
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		log.Debugf("type=%s key=%v value=%x", o, key, Value(b))
		return Value(b)
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil
}

// RESTGet gets value of given key
func (c *Client) RESTGet(key Key) Value {
	c.cid++
	return c.rest(c.ID, key, nil)
}

// RESTPut puts new value as http.request body and return previous value
func (c *Client) RESTPut(key Key, value Value) Value {
	c.cid++
	return c.rest(c.ID, key, value)
}

// Get gets value of given key (use REST)
func (c *Client) Get(key Key) Value {
	return c.RESTGet(key)
}

// Put puts new key value pair and return previous value (use REST)
func (c *Client) Put(key Key, value Value) Value {
	return c.RESTPut(key, value)
}

func (c *Client) json(id ID, key Key, value Value) Value {
	url := c.http[id]
	var cmd Command
	if value == nil {
		cmd = Command{GET, key, value}
	} else {
		cmd = Command{PUT, key, value}
	}
	r := Request{
		ClientID:  c.ID,
		CommandID: c.cid,
		Command:   cmd,
		Timestamp: time.Now().UnixNano(),
	}
	data, err := json.Marshal(r)
	res, err := http.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		log.Errorln(err)
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		log.Debugf("type=%s key=%v value=%x", cmd.Operation, key, Value(b))
		return Value(b)
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil
}

// JSONGet posts get request in json format to server url
func (c *Client) JSONGet(key Key) Value {
	c.cid++
	return c.json(c.ID, key, nil)
}

// JSONPut posts put request in json format to server url
func (c *Client) JSONPut(key Key, value Value) Value {
	c.cid++
	return c.json(c.ID, key, value)
}

// QuorumGet concurrently read values from majority nodes
func (c *Client) QuorumGet(key Key) Value {
	c.cid++
	out := make(chan Value)
	i := 0
	for id := range c.http {
		i++
		if i > c.N/2 {
			break
		}
		go func(id ID) {
			out <- c.rest(id, key, nil)
		}(id)
	}
	set := lib.NewCSet()
	for ; i >= 0; i-- {
		set.Put(<-out)
	}
	if set.Size() == 1 {
		return set.Get().(Value)
	}
	return nil
}

// QuorumPut concurrently write values to majority of nodes
func (c *Client) QuorumPut(key Key, value Value) {
	c.cid++
	var wait sync.WaitGroup
	i := 0
	for id := range c.http {
		i++
		if i > c.N/2 {
			break
		}
		wait.Add(1)
		go func(id ID) {
			c.rest(id, key, value)
			wait.Done()
		}(id)
	}
	wait.Wait()
}

// Start connects to server before actual requests (for future)
func (c *Client) Start() {}

// Stop disconnects from server (for future)
func (c *Client) Stop() {}
