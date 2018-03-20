package wankeeper

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(NewBallot{})
	gob.Register(Vote{})
	gob.Register(NewLeader{})
	gob.Register(Proposal{})
	gob.Register(Ack{})
	gob.Register(Commit{})
	gob.Register(Revoke{})
	gob.Register(Token{})
}

/**************************
 * Intra-Cluster Messages *
 **************************/

// NewBallot message starts a new leader election
type NewBallot struct {
	Ballot paxi.Ballot
}

// Vote message acks NewBallot election
type Vote struct {
	Ballot paxi.Ballot
	ID     paxi.ID
}

func (m Vote) String() string {
	return fmt.Sprintf("Vote {lid=%v, b=%v}", m.Ballot.ID(), m.Ballot)
}

// NewLeader message actives leader
type NewLeader struct {
	Ballot paxi.Ballot
}

func (m NewLeader) String() string {
	return fmt.Sprintf("NewLeader {id=%v, b=%v}", m.Ballot.ID(), m.Ballot)
}

// Proposal from leader to followers
type Proposal struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m Proposal) String() string {
	return fmt.Sprintf("Proposal {bal=%v, slot=%d, cmd=%v}", m.Ballot, m.Slot, m.Command)
}

// Ack from follower to leader
type Ack struct {
	Ballot paxi.Ballot
	ID     paxi.ID
	Slot   int
	Key    paxi.Key
}

func (m Ack) String() string {
	return fmt.Sprintf("Ack {id=%v, bal=%v, slot=%d}", m.ID, m.Ballot, m.Slot)
}

// Commit phase 3
type Commit struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {bal=%v, slot=%d, cmd=%v}", c.Ballot, c.Slot, c.Command)
}

// Token sending between regions
type Token struct {
	Key paxi.Key
}

// Revoke tokens
type Revoke struct {
	Key paxi.Key
}
