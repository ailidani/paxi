package wankeeper

import (
	"fmt"
	. "paxi"
)

/**************************
 * Intra-Cluster Messages *
 **************************/

type NewLeader struct {
	Ballot int
}

func (msg NewLeader) String() string {
	return fmt.Sprintf("NewLeader {lid=%d, bal=%d}", LeaderID(msg.Ballot), msg.Ballot)
}

type Vote struct {
	Ballot int
}

func (msg Vote) String() string {
	return fmt.Sprintf("Vote {lid=%d, bal=%d}", LeaderID(msg.Ballot), msg.Ballot)
}

// Accept phase 2a
type Accept struct {
	Ballot  int
	Slot    int
	Command Command
}

func (a Accept) String() string {
	return fmt.Sprintf("Accept {lid=%s, bal=%d, slot=%d, cmd=%v}", LeaderID(a.Ballot), a.Ballot, a.Slot, a.Command)
}

// Accepted phase 2b
type Accepted struct {
	ID     ID
	Ballot int
	Slot   int
}

func (a Accepted) String() string {
	return fmt.Sprintf("Accepted {lid=%s, bal=%d, slot=%d}", LeaderID(a.Ballot), a.Ballot, a.Slot)
}

// Commit phase 3
type Commit struct {
	Token   Key
	Ballot  int
	Slot    int
	Command Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {token=%d, lid=%s, bal=%d, slot=%d, cmd=%v}", c.Token, LeaderID(c.Ballot), c.Ballot, c.Slot, c.Command)
}
