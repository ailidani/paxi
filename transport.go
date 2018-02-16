package paxi

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"

	"github.com/ailidani/paxi/log"
)

// Transport = transport + pipe + client + server
type Transport interface {
	Scheme() string
	Send(interface{})
	Recv() interface{}
	Dial() error
	Listen()
	Close()
}

// NewTransport creates new transport object with url
func NewTransport(addr string) Transport {
	uri, err := url.Parse(addr)
	if err != nil {
		log.Fatalf("error parsing address %s : %s\n", addr, err)
	}

	transport := &transport{
		uri:   uri,
		codec: NewCodec(Config.Codec),
		send:  make(chan interface{}, Config.ChanBufferSize),
		recv:  make(chan interface{}, Config.ChanBufferSize),
		close: make(chan struct{}),
	}

	switch uri.Scheme {
	case "chan":
		t := new(channel)
		t.transport = transport
		return t
	case "tcp":
		t := new(tcp)
		t.transport = transport
		return t
	case "udp":
		t := new(udp)
		t.transport = transport
		return t
	default:
		log.Fatalf("unknown scheme %s", uri.Scheme)
	}
	return nil
}

type transport struct {
	uri   *url.URL
	codec Codec
	send  chan interface{}
	recv  chan interface{}
	close chan struct{}
}

func (t *transport) Send(m interface{}) {
	select {
	case <-t.close:
		return
	default:
		t.send <- m
	}
}

func (t *transport) Recv() interface{} {
	select {
	case <-t.close:
		return nil
	case m := <-t.recv:
		return m
	}
}

func (t *transport) Close() {
	close(t.close)
}

func (t *transport) Scheme() string {
	return t.uri.Scheme
}

func (t *transport) Dial() error {
	conn, err := net.Dial(t.Scheme(), t.uri.Host)
	if err != nil {
		return err
	}

	go func(conn net.Conn) {
		defer conn.Close()
		// w := bufio.NewWriter(conn)
		var buf bytes.Buffer
		var err error
		for {
			buf.Reset()
			select {
			case <-t.close:
				return
			case m := <-t.send:
				b := t.codec.Encode(m)
				size := uint64(len(b))
				err = binary.Write(&buf, binary.BigEndian, size)
				if err != nil {
					log.Error(err)
					continue
				}
				_, err = buf.Write(b)
				if err != nil {
					log.Error(err)
					continue
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					log.Error(err)
				}
			}
		}
	}(conn)
	return nil
}

/*******************************
/* Intra-process communication *
/*******************************/

var chans = make(map[string]chan interface{})
var chansLock sync.RWMutex

type channel struct {
	*transport
}

func (c *channel) Scheme() string {
	return "chan"
}

func (c *channel) Dial() error {
	chansLock.RLock()
	defer chansLock.RUnlock()
	conn, ok := chans[c.uri.Host]
	if !ok {
		return errors.New("server not ready")
	}
	go func(conn chan<- interface{}) {
		for {
			select {
			case <-c.close:
				return
			case m := <-c.send:
				conn <- m
			}
		}
	}(conn)
	return nil
}

func (c *channel) Listen() {
	chansLock.Lock()
	defer chansLock.Unlock()
	chans[c.uri.Host] = make(chan interface{}, Config.ChanBufferSize)
	go func(conn <-chan interface{}) {
		for {
			select {
			case <-c.close:
				return
			case m := <-conn:
				c.recv <- m
			}
		}
	}(chans[c.uri.Host])
}

/******************************
/*     TCP communication      *
/******************************/
type tcp struct {
	*transport
}

func (t *tcp) Listen() {
	listener, err := net.Listen("tcp", ":"+t.uri.Port())
	if err != nil {
		log.Error("TCP Listener error: ", err)
		log.Fatal("TCP Listener error: ", err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error("TCP Accept error: ", err)
			continue
		}

		go func(conn net.Conn) {
			defer conn.Close()
			//r := bufio.NewReader(conn)
			for {
				select {
				case <-t.close:
					return
				default:
					var size uint64
					err := binary.Read(conn, binary.BigEndian, &size)
					if err != nil {
						log.Error(err)
						return
					}
					if size < 0 || size > 65536 {
						log.Error("TCP reading size error: ", size)
						return
					}
					b := make([]byte, size)
					_, err = io.ReadFull(conn, b)
					if err != nil {
						log.Error(err)
						continue
					}
					t.recv <- t.codec.Decode(b)
				}
			}
		}(conn)
	}
}

/******************************
/*     UDP communication      *
/******************************/
type udp struct {
	*transport
}

func (u *udp) Listen() {
	conn, err := net.ListenPacket("udp", ":"+u.uri.Port())
	if err != nil {
		log.Fatal("UDP Listener error: ", err)
	}
	defer conn.Close()
	packet := make([]byte, 1500)
	for {
		_, _, err := conn.ReadFrom(packet)
		if err != nil {
			log.Error("UDP connection read error: ", err)
			continue
		}
		buf := bytes.NewBuffer(packet)
		var size uint64
		err = binary.Read(buf, binary.BigEndian, &size)
		if err != nil {
			log.Error(err)
			continue
		}
		if size < 0 || size > 1500 {
			log.Error("Size error: ", size)
			continue
		}
		b := make([]byte, size)
		_, err = io.ReadFull(buf, b)
		if err != nil {
			log.Error(err)
			continue
		}
		u.recv <- u.codec.Decode(b)
	}
}
