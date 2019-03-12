package paxi

import (
	"bytes"
	"encoding/gob"
	"errors"
	"flag"
	"net"
	"net/url"
	"strings"
	"sync"

	"github.com/ailidani/paxi/log"
)

var scheme = flag.String("transport", "tcp", "transport scheme (tcp, udp, chan), default tcp")

// Transport = transport + pipe + client + server
type Transport interface {
	// Scheme returns tranport scheme
	Scheme() string

	// Send sends message into t.send chan
	Send(interface{})

	// Recv waits for message from t.recv chan
	Recv() interface{}

	// Dial connects to remote server non-blocking once connected
	Dial() error

	// Listen waits for connections, non-blocking once listener starts
	Listen()

	// Close closes send channel and stops listener
	Close()
}

// NewTransport creates new transport object with url
func NewTransport(addr string) Transport {
	if !strings.Contains(addr, "://") {
		addr = *scheme + "://" + addr
	}
	uri, err := url.Parse(addr)
	if err != nil {
		log.Fatalf("error parsing address %s : %s\n", addr, err)
	}

	transport := &transport{
		uri:   uri,
		send:  make(chan interface{}, config.ChanBufferSize),
		recv:  make(chan interface{}, config.ChanBufferSize),
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
	send  chan interface{}
	recv  chan interface{}
	close chan struct{}
}

func (t *transport) Send(m interface{}) {
	t.send <- m
}

func (t *transport) Recv() interface{} {
	return <-t.recv
}

func (t *transport) Close() {
	close(t.send)
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
		// w := bufio.NewWriter(conn)
		// codec := NewCodec(config.Codec, conn)
		encoder := gob.NewEncoder(conn)
		defer conn.Close()
		for m := range t.send {
			err := encoder.Encode(&m)
			if err != nil {
				log.Error(err)
			}
		}
	}(conn)

	return nil
}

/******************************
/*     TCP communication      *
/******************************/
type tcp struct {
	*transport
}

func (t *tcp) Listen() {
	log.Debug("start listening ", t.uri.Port())
	listener, err := net.Listen("tcp", ":"+t.uri.Port())
	if err != nil {
		log.Fatal("TCP Listener error: ", err)
	}

	go func(listener net.Listener) {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Error("TCP Accept error: ", err)
				continue
			}

			go func(conn net.Conn) {
				// codec := NewCodec(config.Codec, conn)
				decoder := gob.NewDecoder(conn)
				defer conn.Close()
				//r := bufio.NewReader(conn)
				for {
					select {
					case <-t.close:
						return
					default:
						var m interface{}
						err := decoder.Decode(&m)
						if err != nil {
							log.Error(err)
							continue
						}
						t.recv <- m
					}
				}
			}(conn)

		}
	}(listener)
}

/******************************
/*     UDP communication      *
/******************************/
type udp struct {
	*transport
}

func (u *udp) Dial() error {
	addr, err := net.ResolveUDPAddr("udp", u.uri.Host)
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	go func(conn *net.UDPConn) {
		// packet := make([]byte, 1500)
		// w := bytes.NewBuffer(packet)
		w := new(bytes.Buffer)
		for m := range u.send {
			gob.NewEncoder(w).Encode(&m)
			_, err := conn.Write(w.Bytes())
			if err != nil {
				log.Error(err)
			}
			w.Reset()
		}
	}(conn)

	return nil
}

func (u *udp) Listen() {
	addr, err := net.ResolveUDPAddr("udp", ":"+u.uri.Port())
	if err != nil {
		log.Fatal("UDP resolve address error: ", err)
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("UDP Listener error: ", err)
	}
	go func(conn *net.UDPConn) {
		packet := make([]byte, 1500)
		defer conn.Close()
		for {
			select {
			case <-u.close:
				return
			default:
				_, err := conn.Read(packet)
				if err != nil {
					log.Error(err)
					continue
				}
				r := bytes.NewReader(packet)
				var m interface{}
				gob.NewDecoder(r).Decode(&m)
				u.recv <- m
			}
		}
	}(conn)
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
		for m := range c.send {
			conn <- m
		}
	}(conn)
	return nil
}

func (c *channel) Listen() {
	chansLock.Lock()
	defer chansLock.Unlock()
	chans[c.uri.Host] = make(chan interface{}, config.ChanBufferSize)
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
