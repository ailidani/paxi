package dynamo

import (
	"crypto/md5"
	"encoding/binary"

	"github.com/ailidani/paxi"
)

type Replica struct {
	paxi.Node

	index map[paxi.Key]paxi.ID
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:  paxi.NewNode(id),
		index: make(map[paxi.Key]paxi.ID),
	}

	return r
}

// DHT
func index(key paxi.Key) paxi.ID {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(key))
	md5.Sum(b)
}
