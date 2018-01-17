package paxos_group

import (
	"encoding/gob"

	"github.com/ailidani/paxi/paxos"
)

func init() {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Commit{})
}

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
