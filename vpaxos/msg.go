package vpaxos

import (
	"encoding/gob"
	"fmt"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})
	gob.Register(Query{})
	gob.Register(Info{})
	gob.Register(Move{})
}

/**********************
 *   Paxos Messages   *
 **********************/

type Prepare struct {
	GroupID int
	paxos.P1a
}

func (p Prepare) String() string {
	return fmt.Sprintf("Prepare {gid=%v, %v}", p.GroupID, p.P1a)
}

type Promise struct {
	GroupID int
	paxos.P1b
}

type Accept struct {
	GroupID int
	paxos.P2a
}

type Accepted struct {
	GroupID int
	paxos.P2b
}

type Commit struct {
	GroupID int
	paxos.P3
}

/***********************
 *   Master Messages   *
 ***********************/

type Query struct {
	Key paxi.Key
	ID  paxi.ID
}

type Info struct {
	Key    paxi.Key
	Ballot paxi.Ballot
}

type Move struct {
	Key       paxi.Key
	From      paxi.ID
	To        paxi.ID
	OldBallot paxi.Ballot
	NewBallot paxi.Ballot
}
