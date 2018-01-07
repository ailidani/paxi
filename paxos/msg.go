package paxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(P1a{})
	gob.Register(P1b{})
	gob.Register(P2a{})
	gob.Register(P2b{})
	gob.Register(P3{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot        paxi.Ballot
	ID            paxi.ID      // from node id
	Slot          int          // last slot
	Command       paxi.Command // value in last slot
	CommandBallot paxi.Ballot  // value ballot
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v, s=%d, c=%v, cb=%v}", m.Ballot, m.Slot, m.Command, m.CommandBallot)
}

// P2a accept message
type P2a struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%v, s=%d, c=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
	// Command       paxi.Command
	// CommandBallot paxi.Ballot
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v, s=%d}", m.Ballot, m.Slot)
}

// P3 commit message
type P3 struct {
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {s=%d, cmd=%v}", m.Slot, m.Command)
}
