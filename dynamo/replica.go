package dynamo

import (
	"encoding/binary"

	"github.com/ailidani/paxi"
)

type Replica struct {
	paxi.Node

	nodes map[paxi.ID]uint64
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
	return nil
}
