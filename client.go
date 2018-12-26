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
	"encoding/binary"
)

const NO_BARRIER_SLOT = -1

type pqrTuple struct{
	val Value
	slot int
	ID ID
}

// Client main access point of client lib
type Client struct {
	ID        ID // client id use the same id as servers in local site
	N         int
	addrs     map[ID]string
	http      map[ID]string
	algorithm string

	cid int // command id
	*http.Client
}

// NewClient creates a new Client from config
func NewClient(id ID) *Client {
	return &Client{
		ID:        id,
		N:         len(config.Addrs),
		addrs:     config.Addrs,
		http:      config.HTTPAddrs,
		algorithm: config.Algorithm,
		Client:    &http.Client{},
	}
}

func (c *Client) restPaxosQuorumRead(id ID, key Key, barrierSlot int) (Value, int, error) {
	url := c.http[id] + "/pqr/" + strconv.Itoa(int(key))

	method := http.MethodGet
	var body io.Reader
	r, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Error(err)
		return nil, NO_TIMESTAMP, err
	}
	r.Header.Set(HTTPClientID, string(c.ID))
	r.Header.Set(HTTPCommandID, strconv.Itoa(c.cid))
	r.Header.Set(HTTPSlot, strconv.Itoa(barrierSlot))

	res, err := c.Client.Do(r)
	if err != nil {
		log.Error(err)
		return nil, NO_TIMESTAMP, err
	}

	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		strSlot := res.Header.Get(HTTPSlot)
		slot, err := strconv.Atoi(strSlot)
		if err != nil {
			log.Error(err)
			return nil, NO_TIMESTAMP, err
		}
		noVal := res.Header.Get(HTTPNoVal)
		log.Debugf("NoValue Header: %s", noVal)
		if noVal == "" {
			b, err := ioutil.ReadAll(res.Body)
			if err != nil {
				log.Error(err)
				return nil, NO_TIMESTAMP, err
			}
			retVal := Value(b)
			log.Debugf("node=%v type=%s key=%v slot=%d value=%x", id, method, key, slot, retVal)
			return retVal, int(slot), nil
		} else {
			return nil, int(slot), nil
		}

	}

	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil, NO_TIMESTAMP, errors.New(res.Status)
}

// rest accesses server's REST API with url = http://ip:port/key
// if value == nil, it's read
func (c *Client) rest(id ID, key Key, value Value) (Value, error) {
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
func (c *Client) RESTGet(key Key) (Value, error) {
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
func (c *Client) RESTPut(key Key, value Value) (Value, error) {
	c.cid++
	id := c.ID
	if id == "" {
		for id = range c.http {
			break
		}
		log.Debugf("No Client ID. picked id %v\n", id)
	}
	return c.rest(id, key, value)
}

// Get gets value of given key (use REST)
func (c *Client) Get(key Key) (Value, error) {
	if config.PaxosQuorumRead {
		return c.PaxosQuorumGet(key)
	} else {
		return c.RESTGet(key)
	}
}

// Put puts new key value pair and return previous value (use REST)
func (c *Client) Put(key Key, value Value) (Value, error) {
	return c.RESTPut(key, value)
}

func (c *Client) json(id ID, key Key, value Value) (Value, error) {
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
		return Value(b), nil
	}
	dump, _ := httputil.DumpResponse(res, true)
	log.Debugf("%q", dump)
	return nil, errors.New(res.Status)
}

// JSONGet posts get request in json format to server url
func (c *Client) JSONGet(key Key) (Value, error) {
	c.cid++
	return c.json(c.ID, key, nil)
}

// JSONPut posts put request in json format to server url
func (c *Client) JSONPut(key Key, value Value) (Value, error) {
	c.cid++
	return c.json(c.ID, key, value)
}


// PaxosQuorumGet concurrently read values from majority paxos nodes in 2 phases
func (c *Client) paxosQuorumGetPhase(key Key, barrierSlot int) ([]pqrTuple, error) {
	c.cid++
	out := make(chan pqrTuple, c.N/2 + 1)
	i := 0
	for id := range c.http {
		if i > c.N/2 {
			break
		}
		i++
		go func(id ID) {
			log.Debugf("Sending PQR to %v\n", id)
			v, slot, err := c.restPaxosQuorumRead(id, key, barrierSlot)

			if err != nil {
				log.Error(err)
				out <- pqrTuple{nil, -1, ID("0.0")}
				return
			}
			log.Debugf("Received response %d, %d from %v\n", v, slot, id)
			out <- pqrTuple{v, slot, id}
		}(id)
	}
	results := make([]pqrTuple, 0)
	for ; i > 0; i-- {

		res := <-out
		log.Debugf("Collected response %v from %v, awaiting %d more responses\n", res, res.ID, i-1)
		if res.slot == -1 {
			return nil, errors.New("An Error has occured doing PQR")
		}
		results = append(results, res)
	}
	return results, nil
}

func (c *Client) PaxosQuorumGet(key Key) (Value, error) {
	p1Results, err := c.paxosQuorumGetPhase(key, NO_BARRIER_SLOT)
	if err != nil {
		return nil, err
	}

	maxSlot := -1
	valSlot := -1
	var val Value
	val = nil
	valint := 0
	for _, v := range p1Results {
		if v.slot > maxSlot {
			maxSlot = v.slot // v.slot here is last accepted slot at that node
		}
		if v.val != nil && v.slot == maxSlot {
			val = v.val
			valSlot = v.slot // v.slot here is last executed slot
		}
	}
	if len(val) == 10 {
		x, _ := binary.Uvarint(val)
		valint = int(x)
	} else {
		valint = -1
	}
	log.Debugf("Finished PQR phase 1. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
	if maxSlot == valSlot && val != nil {
		log.Debugf("Returning after PQR phase 1. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
		return val, nil
	}

	log.Debugf("Starting PQR phase 2+. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
	val = nil
	for val == nil {
		log.Debugf("Doing PQR phase 2+. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
		p2Results, err := c.paxosQuorumGetPhase(key, maxSlot)
		if err != nil {
			return nil, err
		}
		for _, v := range p2Results {
			if v.slot >= maxSlot{
				val = v.val
				if len(val) == 10 {
					x, _ := binary.Uvarint(val)
					valint = int(x)
				} else {
					valint = -1
				}
				log.Debugf("PQR phase 2+ Value Change. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
				break
			}
		}
	}
	log.Debugf("Returning after PQR phase 2. barrierSlot: %d, val: %d (%v)\n", maxSlot, valint, val)
	return val, nil


}

// QuorumGet concurrently read values from majority nodes
func (c *Client) QuorumGet(key Key) []Value {
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

// Consensus collects /history/key from every node and compare their values
func (c *Client) Consensus(k Key) bool {
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
func (c *Client) Crash(id ID, t int) {
	url := c.http[id] + "/crash?t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Drop drops every message send for t seconds
func (c *Client) Drop(from, to ID, t int) {
	url := c.http[from] + "/drop?id=" + string(to) + "&t=" + strconv.Itoa(t)
	r, err := c.Client.Get(url)
	if err != nil {
		log.Error(err)
		return
	}
	r.Body.Close()
}

// Partition cuts the network between nodes for t seconds
func (c *Client) Partition(t int, nodes ...ID) {
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

// Start connects to server before actual requests (for future)
func (c *Client) Start() {}

// Stop disconnects from server (for future)
func (c *Client) Stop() {}
