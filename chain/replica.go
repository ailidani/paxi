package chain

import (
	"sort"

	"github.com/ailidani/paxi"
)

type entry struct {
	command paxi.Command
	ack     bool
	ballot  paxi.Ballot
	request *paxi.Request
}

type Replica struct {
	paxi.Node

	ballot paxi.Ballot
	head   paxi.ID
	tail   paxi.ID
	prev   paxi.ID
	next   paxi.ID

	log  map[uint64]*entry
	lsn  uint64
	clsn uint64
}

func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.ballot = paxi.NewBallot(1, r.ID())

	ids := make([]paxi.ID, 0)
	for id := range paxi.GetConfig().Addrs {
		ids = append(ids, id)
	}
	sort.Sort(paxi.IDs(ids))

	r.head = ids[0]
	r.tail = ids[len(ids)-1]
	for i := 0; i < len(ids); i++ {
		if ids[i] == r.ID() {
			if i > 0 {
				r.prev = ids[i-1]
			}

			if i < len(ids)-1 {
				r.next = ids[i+1]
			}

			break
		}
	}

	r.log = make(map[uint64]*entry)
	r.lsn = 0

	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(Accept{}, r.handleAccept)
	r.Register(Ack{}, r.handleAck)

	return r
}

func (r *Replica) handleRequest(m paxi.Request) {
	if m.Command.IsRead() && r.tail == r.ID() {
		v := r.Node.Execute(m.Command)
		m.Reply(paxi.Reply{
			Command: m.Command,
			Value:   v,
		})
		return
	}

	if m.Command.IsWrite() && r.head == r.ID() {
		//r.Node.Execute(m.Command)
		r.log[r.lsn] = &entry{
			command: m.Command,
			ack:     false,
			ballot:  r.ballot,
			request: &m,
		}

		r.Send(r.next, Accept{
			Ballot:  r.ballot,
			Command: m.Command,
			LSN:     r.lsn,
			From:    r.ID(),
		})

		r.lsn++
	}
}

func (r *Replica) handleAccept(m Accept) {
	//r.Node.Execute(m.Command)
	r.log[m.LSN] = &entry{
		command: m.Command,
		ack:     false,
		ballot:  m.Ballot,
	}

	if r.tail == r.ID() {
		ack := Ack{
			Ballot: m.Ballot,
			LSN:    m.LSN,
			From:   r.ID(),
		}
		r.log[m.LSN].ack = true
		r.Send(r.head, ack)
		r.Send(r.prev, ack)
	} else {
		r.Send(r.next, Accept{
			Ballot:  m.Ballot,
			Command: m.Command,
			LSN:     m.LSN,
			From:    r.ID(),
		})
	}
}

func (r *Replica) handleAck(m Ack) {
	r.log[m.LSN].ack = true

	if r.head != r.ID() {
		r.Send(r.prev, Ack{
			Ballot: m.Ballot,
			LSN:    m.LSN,
			From:   r.ID(),
		})
	}

	for r.log[r.clsn] != nil && r.log[r.clsn].ack == true {
		e := r.log[r.clsn]
		r.Node.Execute(e.command)

		if r.head == r.ID() && e.request != nil {
			e.request.Reply(paxi.Reply{
				Command: e.command,
			})
			e.request = nil
		}

		r.clsn++
	}
}
