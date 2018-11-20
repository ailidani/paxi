package paxi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"

	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
)

type Client interface {
	Get(Key) (Value, error)
	Put(Key, Value) error
	AdminClient
}

type AdminClient interface {
	Consensus(Key) bool
	Crash(ID, int)
	Drop(ID, ID, int)
	Partition(int, ...ID)
}

// HTTPClient inplements Client interface with REST API
type HTTPClient struct {
	ID    ID // client id use the same id as servers in local site
	N     int
	addrs map[ID]string
	http  map[ID]string

	cid int // command id
	*http.Client
}

// NewHTTPClient creates a new Client from config
func NewHTTPClient(id ID) *HTTPClient {
	return &HTTPClient{
		ID:     id,
		N:      len(config.Addrs),
		addrs:  config.Addrs,
		http:   config.HTTPAddrs,
		Client: &http.Client{},
	}
}

// rest accesses server's REST API with url = http://ip:port/key
// if value == nil, it's read
func (c *HTTPClient) rest(id ID, key Key, value Value) (Value, error) {
	url := c.http[id] + "/" + strconv.Itoa(int(key))

	method := http.MethodGet
	var body io.Reader
	if value != nil {
		method = http.MethodPut
		body = bytes.NewBuffer(value)
	}
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	r.Header.Set(HTTPClientID, string(c.ID))
	r.Header.Set(HTTPCommandID, strconv.Itoa(c.cid))
	// r.Header.Set(HTTPTimestamp, strconv.FormatInt(time.Now().UnixNano(), 10))
	res, err := c.Client.Do(r)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		if value == nil {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, Value(b))
		} else {
			log.Debugf("node=%v type=%s key=%v value=%x", id, method, key, value)
		}
		return Value(b), nil
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil, errors.New(res.Status)
}

// RESTGet gets value of given key
func (c *HTTPClient) RESTGet(key Key) (Value, error) {
	c.cid++
	id := c.ID
	if id == "" {
		for id = range c.http {
			break
		}
	}
	return c.rest(id, key, nil)
}

// RESTPut puts new value as http.request body and return previous value
func (c *HTTPClient) RESTPut(key Key, value Value) (Value, error) {
	c.cid++
	id := c.ID
	if id == "" {
		for id = range c.http {
			break
		}
	}
	return c.rest(id, key, value)
}

// Get gets value of given key (use REST)
func (c *HTTPClient) Get(key Key) (Value, error) {
	return c.RESTGet(key)
}

// Put puts new key value pair and return previous value (use REST)
func (c *HTTPClient) Put(key Key, value Value) error {
	_, err := c.RESTPut(key, value)
	return err
}

func (c *HTTPClient) json(id ID, key Key, value Value) (Value, error) {
	url := c.http[id]
	cmd := Command{
		Key:       key,
		Value:     value,
		ClientID:  c.ID,
		CommandID: c.cid,
	}
	data, err := json.Marshal(cmd)
	res, err := c.Client.Post(url, "json", bytes.NewBuffer(data))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		log.Debugf("key=%v value=%x", key, Value(b))
		return Value(b), nil
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil, errors.New(res.Status)
}

// JSONGet posts get request in json format to server url
func (c *HTTPClient) JSONGet(key Key) (Value, error) {
	c.cid++
	return c.json(c.ID, key, nil)
}

// JSONPut posts put request in json format to server url
func (c *HTTPClient) JSONPut(key Key, value Value) (Value, error) {
	c.cid++
	return c.json(c.ID, key, value)
}

// QuorumGet concurrently read values from majority nodes
func (c *HTTPClient) QuorumGet(key Key) []Value {
	c.cid++
	out := make(chan Value)
	i := 0
	for id := range c.http {
		i++
		if i > c.N/2 {
			break
		}
		go func(id ID) {
			v, err := c.rest(id, key, nil)
			if err != nil {
				log.Error(err)
				return
			}
			out <- v
		}(id)
	}
	set := lib.NewCSet()
	for ; i >= 0; i-- {
		set.Put(<-out)
	}
	res := make([]Value, 0)
	for _, v := range set.Array() {
		res = append(res, v.(Value))
	}
	return res
}

// QuorumPut concurrently write values to majority of nodes
func (c *HTTPClient) QuorumPut(key Key, value Value) {
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

// Consensus collects /history/key from every node and compare their values
func (c *HTTPClient) Consensus(k Key) bool {
	h := make(map[ID][]Value)
	for id, url := range c.http {
		h[id] = make([]Value, 0)
		r, err := c.Client.Get(url + "/history?key=" + strconv.Itoa(int(k)))
		if err != nil {
			log.Error(err)
			continue
		}
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error(err)
			continue
		}
		holder := h[id]
		err = json.Unmarshal(b, &holder)
		if err != nil {
			log.Error(err)
			continue
		}
		h[id] = holder
		log.Debugf("node=%v key=%v h=%v", id, k, holder)
	}
	n := 0
	for _, v := range h {
		if len(v) > n {
			n = len(v)
		}
	}
	for i := 0; i < n; i++ {
		set := make(map[string]struct{})
		for id := range c.http {
			if len(h[id]) > i {
				set[string(h[id][i])] = struct{}{}
			}
		}
		if len(set) > 1 {
			return false
		}
	}
	return true
}

// Crash stops the node for t seconds then recover
// node crash forever if t < 0
func (c *HTTPClient) Crash(id ID, t int) {
	url := c.http[id] + "/crash?t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Drop drops every message send for t seconds
func (c *HTTPClient) Drop(from, to ID, t int) {
	url := c.http[from] + "/drop?id=" + string(to) + "&t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Partition cuts the network between nodes for t seconds
func (c *HTTPClient) Partition(t int, nodes ...ID) {
	s := lib.NewSet()
	for _, id := range nodes {
		s.Add(id)
	}
	for from := range c.addrs {
		if !s.Has(from) {
			for _, to := range nodes {
				c.Drop(from, to, t)
			}
		}
	}
}
