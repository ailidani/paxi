package vpaxos

import (
	"encoding/gob"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})
	gob.Register(Move{})
}

/**********************
 *   Paxos Messages   *
 **********************/

type Prepare struct {
	GroupID int
	paxos.P1a
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
	Key    paxi.Key
	ID     paxi.ID
	Ballot paxi.Ballot
}

type Info struct {
	Key     paxi.Key
	GroupID int
	Ballot  paxi.Ballot
}

type Move struct {
	Key       paxi.Key
	From      int
	To        int
	OldBallot paxi.Ballot
	NewBallot paxi.Ballot
}
