package lib

import (
	"bytes"
	"crypto/md5"
	"fmt"
)

// HashRing implements a hash ring like Chord using md5
type HashRing struct {
	head *hashRingNode
}

type hashRingNode struct {
	hash  []byte
	Value interface{}

	next *hashRingNode
	prev *hashRingNode
}

// Insert inserts the value and its byte into ring as a node
func (h *HashRing) Insert(v interface{}, b []byte) {
	node := new(hashRingNode)
	sum := md5.Sum(b)
	node.hash = sum[:]
	node.Value = v

	i := h.head

	if i == nil {
		node.next = node
		h.head = node
	} else if bytes.Compare(i.hash, node.hash) >= 0 {
		// insert to head
		// move i to tail
		for i.next != h.head {
			i = i.next
		}
		i.next = node
		node.next = h.head
		h.head = node
	} else {
		// move i to node before insertion
		for i.next != h.head && bytes.Compare(i.next.hash, node.hash) < 0 {
			i = i.next
		}
		node.next = i.next
		i.next = node
	}
}

func (h *HashRing) search(hash []byte) *hashRingNode {
	for i := h.head; i.next != h.head; i = i.next {
		if bytes.Compare(hash, i.hash) < 0 {
			return i
		}
	}
	return h.head
}

// Get returns the node value that given k belongs to
func (h *HashRing) Get(k []byte) interface{} {
	sum := md5.Sum(k)
	return h.search(sum[:]).Value
}

// Next returns the next value in the ring; nil if v does not exists
func (h *HashRing) Next(v interface{}) interface{} {
	for i := h.head; ; i = i.next {
		if v == i.Value {
			return i.next.Value
		}
		if i.next == h.head {
			return nil
		}
	}
}

func (h HashRing) String() string {
	if h.head == nil {
		return ""
	}
	var buffer bytes.Buffer
	for i := h.head; ; i = i.next {
		s := fmt.Sprintf("%+v -> ", i.Value)
		buffer.Write([]byte(s))
		if i.next == h.head {
			break
		}
	}
	return buffer.String()
}
