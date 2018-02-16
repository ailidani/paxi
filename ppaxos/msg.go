package ppaxos

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
	gob.Register(LeaderChange{})
}

// P1a message for given key
type P1a struct {
	Key    paxi.Key
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {key=%v b=%v}", m.Key, m.Ballot)
}

// CommandBallot not used
type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("c=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Key    paxi.Key
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
	Value  paxi.Value
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {key=%v b=%v s=%d v=%x}", m.Key, m.Ballot, m.Slot, m.Value)
}

// P2a accept message
type P2a struct {
	Key     paxi.Key
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m P2a) String() string {
	return fmt.Sprintf("P2a {key=%v b=%v s=%v cmd=%v}", m.Key, m.Ballot, m.Slot, m.Command)
}

// P2b accepted message
type P2b struct {
	Key    paxi.Key
	Ballot paxi.Ballot
	ID     paxi.ID // from node id
	Slot   int
	Value  paxi.Value
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {key=%v b=%v s=%v}", m.Key, m.Ballot, m.Slot)
}

// LeaderChange switch leader
type LeaderChange struct {
	Key    paxi.Key
	To     paxi.ID
	From   paxi.ID
	Ballot paxi.Ballot
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {key=%d, from=%s, to=%s, bal=%d}", l.Key, l.From, l.To, l.Ballot)
}
