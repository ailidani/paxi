package kpaxos

import (
	"encoding/gob"
	. "paxi"
	"paxi/glog"
)

type Replica struct {
	*Node
	paxi map[Key]*paxos
}

func NewReplica(config *Config) *Replica {
	gob.Register(Prepare{})
	gob.Register(Promise{})
	gob.Register(Accept{})
	gob.Register(Accepted{})
	gob.Register(Nack{})
	gob.Register(Commit{})

	return &Replica{
		Node: NewNode(config),
		paxi: make(map[Key]*paxos),
	}
}

// Run start running replica
func (r *Replica) Run() {
	go r.messageLoop()
	r.Node.Run()
}

func (r *Replica) messageLoop() {
	for {
		select {
		case msg := <-r.RequestChan:
			glog.V(2).Infof("Replica %s received %v\n", r.ID, msg)
			r.handleRequest(msg)

		case msg := <-r.MessageChan:
			r.dispatch(msg)
		}
	}
}

func (r *Replica) dispatch(msg Message) {
	switch msg := msg.(type) {
	case Prepare:
		glog.V(1).Infof("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
		r.handlePrepare(msg)

	case Promise:
		glog.V(1).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.ID, msg, r.ID)
		r.handlePromise(msg)
		glog.V(2).Infof("Number of keys: %d", r.keys())

	case Accept:
		glog.V(1).Infof("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
		r.handleAccept(msg)

	case Accepted:
		glog.V(1).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.ID, msg, r.ID)
		r.handleAccepted(msg)

	case Commit:
		// glog.V(1).Infof("Replica %s ===[%v]===>>> Replica %s\n", LeaderID(msg.Ballot), msg, r.ID)
		r.handleCommit(msg)
	}
}

func index(key Key) ID {
	if key < 200 {
		return NewID(1, 1)
	} else if key >= 200 && key < 400 {
		return NewID(2, 1)
	} else if key >= 400 && key < 600 {
		return NewID(3, 1)
	} else if key >= 600 && key < 800 {
		return NewID(4, 1)
	} else {
		return NewID(5, 1)
	}
}

func (r *Replica) init(key Key) {
	if _, exists := r.paxi[key]; !exists {
		r.paxi[key] = NewPaxos(r.Node, key)
		id := index(key)
		if id == r.ID {
			r.paxi[key].active = true
		}
		r.paxi[key].ballot = NextBallot(1, id)
	}
}

func (r *Replica) handleRequest(msg Request) {
	key := msg.Command.Key
	r.init(key)
	r.paxi[key].handleRequest(msg)
}

func (r *Replica) handlePrepare(msg Prepare) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handlePrepare(msg)
}

func (r *Replica) handlePromise(msg Promise) {
	key := msg.Key
	r.paxi[key].handlePromise(msg)
}

func (r *Replica) handleAccept(msg Accept) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handleAccept(msg)
}

func (r *Replica) handleAccepted(msg Accepted) {
	key := msg.Key
	r.paxi[key].handleAccepted(msg)
}

func (r *Replica) handleCommit(msg Commit) {
	key := msg.Key
	r.init(key)
	r.paxi[key].handleCommit(msg)
}

func (r *Replica) keys() int {
	sum := 0
	for _, paxos := range r.paxi {
		if paxos.active {
			sum++
		}
	}
	return sum
}
