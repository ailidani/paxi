package ppaxos

import (
	"encoding/gob"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
}

type P1a struct {
	Key    paxi.Key
	Ballot paxi.Ballot
}

type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

// P1b promise message
type P1b struct {
	Key    paxi.Key
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
}

// P2a accept message
type P2a struct {
	Key     paxi.Key
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

// P2b accepted message
type P2b struct {
	Key    paxi.Key
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
}
