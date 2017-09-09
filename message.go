package paxi

import (
	"sync"
	"sync/atomic"
	"time"
)

const HEADER_SIZE int = 32

/***************************
 * Transport layer message *
 ***************************/

type Message struct {
	Header []byte
	Body   []byte

	bbuf   []byte
	hbuf   []byte
	bsize  int
	refcnt int32
	expire time.Time
	pool   *sync.Pool
}

type msgCacheInfo struct {
	maxbody int
	pool    *sync.Pool
}

func newMsg(sz int) *Message {
	m := &Message{}
	m.bbuf = make([]byte, 0, sz)
	m.hbuf = make([]byte, 0, HEADER_SIZE)
	m.bsize = sz
	return m
}

// We can tweak these!
var messageCache = []msgCacheInfo{
	{
		maxbody: 64,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(64) },
		},
	}, {
		maxbody: 128,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(128) },
		},
	}, {
		maxbody: 256,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(256) },
		},
	}, {
		maxbody: 512,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(512) },
		},
	}, {
		maxbody: 1024,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(1024) },
		},
	}, {
		maxbody: 4096,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(4096) },
		},
	}, {
		maxbody: 8192,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(8192) },
		},
	}, {
		maxbody: 65536,
		pool: &sync.Pool{
			New: func() interface{} { return newMsg(65536) },
		},
	},
}

func (m *Message) Free() {
	if v := atomic.AddInt32(&m.refcnt, -1); v > 0 {
		return
	}
	for i := range messageCache {
		if m.bsize == messageCache[i].maxbody {
			messageCache[i].pool.Put(m)
			return
		}
	}
}

func (m *Message) Dup() *Message {
	atomic.AddInt32(&m.refcnt, 1)
	return m
}

func (m *Message) Expired() bool {
	if m.expire.IsZero() {
		return false
	}
	if m.expire.After(time.Now()) {
		return false
	}
	return true
}

func NewMessage(sz int) *Message {
	var m *Message
	for i := range messageCache {
		if sz < messageCache[i].maxbody {
			m = messageCache[i].pool.Get().(*Message)
			break
		}
	}
	if m == nil {
		m = newMsg(sz)
	}

	m.refcnt = 1
	m.Body = m.bbuf
	m.Header = m.hbuf
	return m
}
