package atomic

import (
	"encoding/gob"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(Get{})
	gob.Register(GetReply{})
	gob.Register(Set{})
	gob.Register(SetReply{})
}

type Get struct {
	ID  paxi.ID
	CID int
	Key paxi.Key
}

type GetReply struct {
	ID      paxi.ID
	CID     int
	Key     paxi.Key
	Value   paxi.Value
	Version int
}

type Set struct {
	ID      paxi.ID
	CID     int
	Key     paxi.Key
	Value   paxi.Value
	Version int
}

// SetReply acknowledges a set operation, whether succeed or not
type SetReply struct {
	ID  paxi.ID
	CID int
	Key paxi.Key
}
