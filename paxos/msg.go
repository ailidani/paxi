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
	Ballot int // <n, id>
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%d}", m.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot  int
	ID      paxi.ID      // from node id
	Slot    int          // last slot
	Command paxi.Command // cmd in last slot
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%d, s=%d, cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2a accept message
type P2a struct {
	Ballot  int
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {b=%d, s=%d, cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Ballot int
	Slot   int
	ID     paxi.ID // from node id
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%d, s=%d}", m.Ballot, m.Slot)
}

// P3 commit message
type P3 struct {
	Ballot  int
	Slot    int
	Command paxi.Command
}

func (m P3) String() string {
	return fmt.Sprintf("P3 {b=%d, s=%d, cmd=%v}", m.Ballot, m.Slot, m.Command)
}
