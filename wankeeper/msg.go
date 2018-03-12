package wankeeper

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(NewLeader{})
	gob.Register(Vote{})
	gob.Register(Proposal{})
	gob.Register(Ack{})
	gob.Register(Commit{})
	gob.Register(Revoke{})
	gob.Register(Token{})
}

/**************************
 * Intra-Cluster Messages *
 **************************/

// NewLeader message
type NewLeader struct {
	Ballot paxi.Ballot
}

func (m NewLeader) String() string {
	return fmt.Sprintf("NewLeader {id=%v, b=%v}", m.Ballot.ID(), m.Ballot)
}

// Vote message acks NewLeader election
type Vote struct {
	Ballot paxi.Ballot
	ID     paxi.ID
}

func (m Vote) String() string {
	return fmt.Sprintf("Vote {lid=%v, b=%v}", m.Ballot.ID(), m.Ballot)
}

// Proposal from leader to followers
type Proposal struct {
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (m Proposal) String() string {
	return fmt.Sprintf("Accept {lid=%v, bal=%v, slot=%d, cmd=%v}", m.Ballot.ID(), m.Ballot, m.Slot, m.Command)
}

// Ack from follower to leader
type Ack struct {
	Ballot paxi.Ballot
	ID     paxi.ID
	Slot   int
	Key    paxi.Key
}

func (m Ack) String() string {
	return fmt.Sprintf("Accepted {lid=%v, bal=%v, slot=%d}", m.Ballot.ID(), m.Ballot, m.Slot)
}

// Commit phase 3
type Commit struct {
	Token   paxi.Key
	Ballot  paxi.Ballot
	Slot    int
	Command paxi.Command
}

func (c Commit) String() string {
	return fmt.Sprintf("Commit {token=%d, lid=%v, bal=%v, slot=%d, cmd=%v}", c.Token, c.Ballot.ID(), c.Ballot, c.Slot, c.Command)
}

// Token sending between regions
type Token struct {
	Token paxi.Key
}

// Revoke tokens
type Revoke struct {
	Token paxi.Key
}
