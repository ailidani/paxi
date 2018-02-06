package async_paxos

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
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(LeaderChange{})
}

// P1a prepare message
type P1a struct {
	Ballot paxi.Ballot
}

func (m P1a) String() string {
	return fmt.Sprintf("P1a {b=%v}", m.Ballot)
}

type CommandBallot struct {
	Command paxi.Command
	Ballot  paxi.Ballot
}

func (cb CommandBallot) String() string {
	return fmt.Sprintf("c=%v b=%v", cb.Command, cb.Ballot)
}

// P1b promise message
type P1b struct {
	Ballot paxi.Ballot
	ID     paxi.ID               // from node id
	Log    map[int]CommandBallot // uncommitted logs
}

func (m P1b) String() string {
	return fmt.Sprintf("P1b {b=%v, log=%v}", m.Ballot, m.Log)
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
}

func (m P2b) String() string {
	return fmt.Sprintf("P2b {b=%v, s=%d}", m.Ballot, m.Slot)
}

// Prepare phase 1a
type Prepare struct {
	Key paxi.Key
	P1a
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {key=%v, %v}", p.Key, p.P1a)
}

// Promise phase 1b
type Promise struct {
	Key paxi.Key
	P1b
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {key=%v, %v}", p.Key, p.P1b)
}

// Accept phase 2a
type Accept struct {
	Key paxi.Key
	P2a
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {key=%d, %v}", a.Key, a.P2a)
}

// Accepted phase 2b
type Accepted struct {
	Key paxi.Key
	P2b
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {key=%v, %v}", a.Key, a.P2b)
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
