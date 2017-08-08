package wpaxos

import (
	"fmt"
	. "paxi"
)

/**************************
 * Inter-Replica Messages *
 **************************/

// Prepare phase 1a
type Prepare struct {
	Key    Key
	Ballot int
	// Slot int
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {key=%v, lid=%v, bal=%d}", p.Key, LeaderID(p.Ballot), p.Ballot)
}

// Promise phase 1b
type Promise struct {
	Key     Key
	ID      ID
	Ballot  int
	PreSlot int
}

func (p Promise) String() string {
	return fmt.Sprintf("Promise {key=%v, lid=%s, bal=%d, ps=%d}", p.Key, LeaderID(p.Ballot), p.Ballot, p.PreSlot)
}

// Nack is used as reject in both phase 1 and phase 2
type Nack struct {
	Key    Key
	ID     ID
	Ballot int
}

// Accept phase 2a
type Accept struct {
	Key     Key
	Ballot  int
	Slot    int
	Command Command
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {key=%d, lid=%s, bal=%d, slot=%d, cmd=%v}", a.Key, LeaderID(a.Ballot), a.Ballot, a.Slot, a.Command)
}

// Accepted phase 2b
type Accepted struct {
	Key    Key
	ID     ID
	Ballot int
	Slot   int
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {key=%v, lid=%s, bal=%d, slot=%d}", a.Key, LeaderID(a.Ballot), a.Ballot, a.Slot)
}

// Commit phase 3
type Commit struct {
	Key     Key
	Ballot  int
	Slot    int
	Command Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {key=%d, lid=%s, bal=%d, slot=%d, cmd=%v}", c.Key, LeaderID(c.Ballot), c.Ballot, c.Slot, c.Command)
}

// LeaderChange switch leader
type LeaderChange struct {
	Key    Key
	To     ID
	From   ID
	Ballot int
}

func (l LeaderChange) String() string {
	return fmt.Sprintf("LeaderChange {key=%d, from=%s, to=%s, bal=%d}", l.Key, l.From, l.To, l.Ballot)
}
