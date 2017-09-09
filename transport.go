package paxi

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"net/url"
	"paxi/glog"
)

// Transport = transport + pipe + client + server
type Transport interface {
	Scheme() string
	Send(*Message)
	Recv() *Message
	Dial() error
	Listen()
	Close()
}

// NewTransport creates new transport object with url
func NewTransport(addr string) Transport {
	uri, err := url.Parse(addr)
	if err != nil {
		glog.Error("error parsing address %s", addr)
	}

	transport := new(transport)
	transport.uri = uri
	transport.send = make(chan *Message)
	transport.recv = make(chan *Message)
	transport.close = make(chan struct{})

	switch uri.Scheme {
	case "chan":
		t := new(channel)
		t.transport = transport
		t.addr = uri.Host
		return t
	case "tcp":
		t := new(tcp)
		t.transport = transport
		return t
	case "udp":
		t := new(udp)
		t.transport = transport
		return t
	}
	return nil
}

type transport struct {
	uri   *url.URL
	send  chan *Message
	recv  chan *Message
	close chan struct{}
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
		//w := bufio.NewWriter(conn)
		var buf bytes.Buffer
		var err error
		for {
			buf.Reset()
			select {
			case <-t.close:
				return
			case m := <-t.send:
				if m.Expired() {
					m.Free()
					continue
				}
				size := uint64(len(m.Header) + len(m.Body))
				err = binary.Write(&buf, binary.BigEndian, size)
				if err != nil {
					glog.Errorln(err)
					m.Free()
					return
				}
				if len(m.Header) > 0 {
					_, err = buf.Write(m.Header)
					if err != nil {
						glog.Errorln(err)
						m.Free()
						return
					}
				}
				_, err = buf.Write(m.Body)
				if err != nil {
					glog.Errorln(err)
					m.Free()
					return
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					glog.Errorln(err)
					m.Free()
					return
				}
				m.Free()
			}
		}
	}(conn)
	return nil
}

func (t *transport) Send(m *Message) {
	select {
	case <-t.close:
		return
	default:
		t.send <- m
	}
}

func (t *transport) Recv() *Message {
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

/******************************
/* Inta-process communication *
/******************************/

type channel struct {
	*transport
	addr string
	peer string
}

func (c *channel) Scheme() string {
	return "chan"
}

func (c *channel) Dial() error {
	return nil
}

func (c *channel) Listen() {}

/******************************
/*     TCP communication      *
/******************************/
type tcp struct {
	*transport
}

func (t *tcp) Listen() {
	listener, err := net.Listen("tcp", t.uri.Host)
	if err != nil {
		glog.Fatalln("TCP Listener error: ", err)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			glog.Errorln("TCP Accept error: ", err)
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
						glog.Errorln(err)
						return
					}
					if size < 0 || size > 65536 {
						glog.Errorln("TCP reading size error: ", size)
						return
					}
					m := NewMessage(int(size))
					m.Body = m.Body[0:size]
					_, err = io.ReadFull(conn, m.Body)
					if err != nil {
						glog.Errorln(err)
						m.Free()
						return
					}
					t.recv <- m
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
	conn, err := net.ListenPacket("udp", u.uri.Host)
	if err != nil {
		glog.Fatal("UDP Listener error: ", err)
	}
	defer conn.Close()
	b := make([]byte, 1024)
	for {
		_, _, err := conn.ReadFrom(b)
		if err != nil {
			glog.Errorln("UDP connection read error: ", err)
			continue
		}
		buf := bytes.NewBuffer(b)
		var size uint64
		err = binary.Read(buf, binary.BigEndian, &size)
		if err != nil {
			glog.Errorln(err)
			continue
		}
		if size < 0 || size > 1500 {
			glog.Errorln("Size error: ", size)
			continue
		}
		m := NewMessage(int(size))
		m.Body = m.Body[0:int(size)]
		_, err = io.ReadFull(buf, m.Body)
		if err != nil {
			glog.Errorln(err)
			m.Free()
			continue
		}
		u.recv <- m
	}
}
