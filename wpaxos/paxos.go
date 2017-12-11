package wpaxos

import (
	"math/rand"
	"time"

	. "github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)

type instance struct {
	ballot    int
	commands  []Command
	committed bool
	request   *Request
	quorum    *Quorum
	timestamp time.Time
}

type paxos struct {
	*Replica

	key      Key
	active   bool
	ballot   int
	quorum   *Quorum   // quorum for phase 1
	requests []Request // pending requests in phase 1
	sleeping bool

	log    map[int]*instance
	slot   int
	commit int

	stat *stat
}

func NewPaxos(replica *Replica, key Key) *paxos {
	return &paxos{
		Replica:  replica,
		key:      key,
		active:   false,
		ballot:   0,
		requests: make([]Request, 0),
		log:      make(map[int]*instance),
		slot:     0,
		commit:   0,
		stat:     NewStat(replica.Config.Threshold),
	}
}

// phase 1
func (p *paxos) prepare() {
	if p.active == false {
		p.NextBallot()
		p.quorum = NewQuorum()
		p.quorum.ACK(p.ID)
		p.Broadcast(&Prepare{
			Key:    p.key,
			Ballot: p.ballot,
		})
		log.Debugf("Replica %v sent %v", p.ID, Prepare{p.key, p.ballot})
	}
}

// phase 2
func (p *paxos) accept(msg Request) {
	p.slot++
	p.log[p.slot] = &instance{
		ballot:    p.ballot,
		commands:  []Command{msg.Command},
		committed: false,
		request:   &msg,
		quorum:    NewQuorum(),
		timestamp: time.Now(),
	}
	p.log[p.slot].quorum.ACK(p.ID)
	p.Broadcast(&Accept{
		Key:      p.key,
		Ballot:   p.ballot,
		Slot:     p.slot,
		Commands: []Command{msg.Command},
	})
}

func (p *paxos) handleRequest(msg Request) {
	if p.active {
		p.accept(msg)
		to := p.stat.hit(NewID(msg.ClientID.Zone(), 1))
		if p.Config.Threshold > 0 && to != 0 && to.Zone() != p.ID.Zone() {
			p.Send(to, &LeaderChange{
				Key:    p.key,
				To:     to,
				From:   p.ID,
				Ballot: p.ballot,
			})
		}
	} else if LeaderID(p.ballot) == p.ID {
		p.requests = append(p.requests, msg)
	} else if p.Config.Threshold > 0 && p.ballot != 0 {
		// p.Forward(LeaderID(p.ballot), msg)
		rep := Reply{
			OK:        false,
			CommandID: msg.CommandID,
			LeaderID:  LeaderID(p.ballot),
			ClientID:  msg.ClientID,
			Command:   msg.Command,
			Timestamp: msg.Timestamp,
		}
		msg.Reply(rep)
	} else {
		p.requests = append(p.requests, msg)
		p.NextBallot()
		p.prepare()
	}
}

func (p *paxos) handlePrepare(msg Prepare) {
	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
		p.active = false
		if len(p.requests) > 0 && !p.sleeping {
			// p.prepare()
			p.sleeping = true
			go func() {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+p.Config.BackOff))
				p.prepare()
				p.sleeping = false
			}()
		}
	}

	p.Send(LeaderID(msg.Ballot), &Promise{
		Key:     p.key,
		ID:      p.ID,
		Ballot:  p.ballot,
		PreSlot: p.slot,
	})
}

func (p *paxos) handlePromise(msg Promise) {
	if msg.Ballot < p.ballot || p.active {
		log.Debugf("Replica %s Ignoring old Promise msg %v\n", p.ID, msg)
		return
	}

	// TODO update commands too not just slot
	if msg.PreSlot > p.slot {
		p.slot = msg.PreSlot
	}

	if msg.Ballot == p.ballot && LeaderID(msg.Ballot) == p.ID {
		p.quorum.ACK(msg.ID)
		if p.quorum.Q1() {
			p.active = true
			for _, req := range p.requests {
				p.accept(req)
			}
			p.requests = make([]Request, 0)
		}
	} else {
		p.ballot = msg.Ballot
		p.active = false
		// p.prepare()
		if !p.sleeping {
			p.sleeping = true
			go func() {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+p.Config.BackOff))
				p.prepare()
				p.sleeping = false
			}()
		}

	}
}

func (p *paxos) handleAccept(msg Accept) {
	if msg.Ballot >= p.ballot {
		p.ballot = msg.Ballot
		p.active = false
		p.slot = Max(p.slot, msg.Slot)

		ins, exists := p.log[msg.Slot]
		if exists && ins.request != nil {
			p.MessageChan <- *p.log[msg.Slot].request
		}
		p.log[msg.Slot] = &instance{
			ballot:    msg.Ballot,
			commands:  msg.Commands,
			committed: false,
		}
	}

	p.Send(LeaderID(msg.Ballot), &Accepted{
		Key:    p.key,
		ID:     p.ID,
		Ballot: p.ballot,
		Slot:   msg.Slot,
	})
}

func (p *paxos) handleAccepted(msg Accepted) {
	ins := p.log[msg.Slot]
	if ins == nil {
		log.Warningf("Unknown Accepted msg %v\n", msg)
		return
	}

	if ins.committed || msg.Ballot < ins.ballot {
		log.Debugf("Ignore old Accepted msg %v\n", msg)
		return
	}

	if msg.Ballot == ins.ballot {
		ins.quorum.ACK(msg.ID)

		if ins.quorum.Q2() {
			ins.committed = true
			for p.log[p.commit+1] != nil && p.log[p.commit+1].committed {
				p.commit++
			}
			p.Broadcast(&Commit{
				Key:      p.key,
				Ballot:   ins.ballot,
				Slot:     msg.Slot,
				Commands: ins.commands,
			})
			if p.Config.ReplyWhenCommit {
				rep := Reply{
					OK:        true,
					CommandID: ins.request.CommandID,
					LeaderID:  p.ID,
					ClientID:  ins.request.ClientID,
					Command:   ins.request.Command,
					Timestamp: ins.request.Timestamp,
				}
				ins.request.Reply(rep)
			}
			p.exec()
		}
	} else {
		log.Warningf("Replica %s put cmd %v in slot=%d back to queue.\n", p.ID, ins.request.Command, msg.Slot)
		p.MessageChan <- *ins.request
		delete(p.log, msg.Slot)
	}

	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
		p.active = false
	}
}

func (p *paxos) handleCommit(msg Commit) {
	if msg.Ballot > p.ballot {
		p.ballot = msg.Ballot
	}

	if msg.Slot > p.slot {
		p.slot = msg.Slot
	}

	if p.log[msg.Slot] == nil {
		p.log[msg.Slot] = &instance{
			commands:  msg.Commands,
			ballot:    msg.Ballot,
			committed: true,
		}
	} else {
		p.log[msg.Slot].committed = true
	}

	// for p.log[p.commit+1] != nil && p.log[p.commit+1].committed {
	// 	p.commit++
	// }

	p.exec()
}

func (p *paxos) handleLeaderChange(msg LeaderChange) {
	// msg.From == LeaderID(p.ballot) ???
	if msg.To == p.ID {
		log.Debugf("Replica %s : change leader of key %d\n", p.ID, p.key)
		p.ballot = Max(p.ballot, msg.Ballot)
		p.prepare()
	}
}

func (p *paxos) NextBallot() {
	p.ballot = NextBallot(p.ballot, p.ID)
}

// TODO use exec() instead of reply on commit
func (p *paxos) exec() {
	for {
		i, ok := p.log[p.commit]
		if !ok || !i.committed {
			break
		}
		log.Debugf("execute cmd=%v in slot %d ", i.commands[0], p.commit)
		value, err := p.DB.Execute(i.commands[0])
		p.commit++
		if i.request != nil && !p.Config.ReplyWhenCommit {
			reply := new(Reply)
			reply.ClientID = i.request.ClientID
			reply.CommandID = i.request.CommandID
			i.request.Command.Value = value
			reply.Command = i.request.Command
			reply.Err = err
			i.request.Reply(*reply)
		}
	}
}
