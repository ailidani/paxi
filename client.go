package paxi

import (
	"encoding/gob"
	"net"
	"paxi/glog"
	"sync"
	"time"
)

type Client struct {
	ID       ID
	N        int
	addrs    map[ID]string
	servers  map[ID]net.Conn
	encoders map[ID]*gob.Encoder
	decoders map[ID]*gob.Decoder

	index map[Key]ID
	cid   CommandID

	sendChan chan *Request
	recvChan chan *Reply

	requests map[CommandID]*Request
	results  map[CommandID]bool

	sync.RWMutex
	sync.WaitGroup
}

func NewClient(config *Config) *Client {

	gob.Register(Request{})
	gob.Register(Reply{})
	gob.Register(Register{})

	n := len(config.Addrs)
	return &Client{
		ID:       config.ID,
		N:        n,
		addrs:    config.Addrs,
		servers:  make(map[ID]net.Conn, n),
		encoders: make(map[ID]*gob.Encoder, n),
		decoders: make(map[ID]*gob.Decoder, n),
		index:    make(map[Key]ID),
		cid:      0,
		sendChan: make(chan *Request, config.ChanBufferSize),
		recvChan: make(chan *Reply, config.ChanBufferSize),
		requests: make(map[CommandID]*Request, config.BufferSize),
		results:  make(map[CommandID]bool, config.BufferSize),
	}
}

func (c *Client) Start() {
	for id, addr := range c.addrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			glog.Errorf("Error connecting to replica %s addr=%s.\n", id, addr)
			glog.Fatalf("Error connecting to replica %s addr=%s.\n", id, addr)
		}
		c.Lock()
		c.servers[id] = conn
		c.decoders[id] = gob.NewDecoder(conn)
		c.encoders[id] = gob.NewEncoder(conn)
		c.Unlock()
		var msg Message
		msg = &Register{
			EndpointType: CLIENT,
			ID:           c.ID,
			Addr:         "",
		}
		c.encoders[id].Encode(&msg)
		go c.recvLoop(id)
	}
	go c.sendLoop()
}

func (c *Client) Stop() {
	close(c.recvChan)
	for _, conn := range c.servers {
		conn.Close()
	}
}

func (c *Client) sendLoop() {
	for {
		p := <-c.sendChan
		c.RLock()
		id, exists := c.index[p.Command.Key]
		c.RUnlock()
		if !exists {
			id = NewID(c.ID.Site(), 1)
		}
		// Sending
		var msg Message = p
		c.RLock()
		if _, ok := c.encoders[id]; !ok {
			id = NewID(c.ID.Site(), 2)
		}
		err := c.encoders[id].Encode(&msg)
		c.RUnlock()
		if err != nil {
			glog.Errorln("Client send error", err)
			c.failed(id)
		}
	}
}

func (c *Client) failed(id ID) {
	c.Lock()
	defer c.Unlock()
	delete(c.encoders, id)
	delete(c.decoders, id)
	delete(c.servers, id)
}

func (c *Client) recvLoop(id ID) {
	c.RLock()
	decoder := c.decoders[id]
	c.RUnlock()
	for {
		var reply Reply
		err := decoder.Decode(&reply)
		if err != nil {
			glog.Errorln("Client receive error", err)
			c.failed(id)
			break
		}
		// glog.V(2).Infof("Received %s\n", reply)

		if reply.OK {
			glog.V(2).Infof("cmd %v finished in %f ms.\n", reply.Command, float64(time.Now().UnixNano()-reply.Timestamp)/1000000.0)
			c.Lock()
			c.results[reply.CommandID] = true
			c.Unlock()
			c.recvChan <- &reply
			c.Done()
		} else {
			// glog.V(2).Infof("Client resending cmd %v to replica %s\n", reply.Command, reply.LeaderID)
			c.Lock()
			c.index[reply.Command.Key] = reply.LeaderID
			c.Unlock()
			// Resending
			c.sendChan <- &Request{reply.CommandID, reply.Command, c.ID, reply.Timestamp}
		}
	}
}

func (c *Client) Put(key Key, value Value) {
	c.PutAsync(key, value)
	<-c.recvChan
}

func (c *Client) Get(key Key) {
	c.GetAsync(key)
	<-c.recvChan
}

func (c *Client) PutAsync(key Key, value Value) {
	c.Add(1)
	cmd := Command{PUT, key, value}
	c.cid++
	proposal := &Request{c.cid, cmd, c.ID, time.Now().UnixNano()}
	c.Lock()
	c.results[c.cid] = false
	c.Unlock()
	c.sendChan <- proposal
}

func (c *Client) GetAsync(key Key) {
	c.Add(1)
	cmd := Command{GET, key, NIL}
	c.cid++
	proposal := &Request{c.cid, cmd, c.ID, time.Now().UnixNano()}
	c.Lock()
	c.results[c.cid] = false
	c.Unlock()
	c.sendChan <- proposal
}

func (c *Client) Successful() int {
	sum := 0
	for _, succeed := range c.results {
		if succeed {
			sum++
		}
	}
	return sum
}
