package abd

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

// Get message
type Get struct {
	ID  paxi.ID
	CID int
	Key paxi.Key
}

// GetReply message returns value and version
type GetReply struct {
	ID      paxi.ID
	CID     int
	Key     paxi.Key
	Value   paxi.Value
	Version int
}

// Set message
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
