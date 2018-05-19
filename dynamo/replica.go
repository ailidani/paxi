package dynamo

import (
	"encoding/binary"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/lib"
	"github.com/ailidani/paxi/log"
)

type Replica struct {
	paxi.Node

	ring  *lib.HashRing
	index map[paxi.Key]paxi.ID
}

func NewReplica(id paxi.ID) *Replica {
	r := &Replica{
		Node:  paxi.NewNode(id),
		ring:  new(lib.HashRing),
		index: make(map[paxi.Key]paxi.ID),
	}

	for id := range paxi.GetConfig().Addrs {
		r.ring.Insert(id, []byte(id))
	}
	log.Debug(r.ring)

	r.Register(paxi.Request{}, r.HandleRequest)
	r.Register(Replicate{}, r.HandleReplicate)
	return r
}

// DHT
func (r *Replica) hash(key paxi.Key) paxi.ID {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(key))
	return r.ring.Get(b).(paxi.ID)
}

// replicas returns the next 2 neighbors of current node as its replicas
func (r *Replica) replicas(id paxi.ID) []paxi.ID {
	replicas := make([]paxi.ID, 0)
	for i := 0; i < 2; i++ {
		if r.ring.Next(id) == nil {
			log.Errorf("next of id %v is nil", id)
		}
		next := r.ring.Next(id).(paxi.ID)
		replicas = append(replicas, next)
		id = next
	}
	return replicas
}

// HandleRequest handles read request if node is one of replica, otherwise forward to random replica
// write request will only write to first node in replica set, and replicate asynchronously
func (r *Replica) HandleRequest(m paxi.Request) {
	key := m.Command.Key
	id, exists := r.index[key]
	if !exists {
		id = r.hash(key)
		r.index[key] = id
	}

	replicas := r.replicas(id)
	if m.Command.IsRead() {
		var replica paxi.ID
		for _, replica = range replicas {
			if r.ID() == replica {
				v := r.Node.Execute(m.Command)
				m.Reply(paxi.Reply{
					Command: m.Command,
					Value:   v,
				})
				return
			}
		}
		go r.Forward(replica, m)
	} else {
		if id == r.ID() {
			v := r.Node.Execute(m.Command)
			for _, id := range r.replicas(r.ID()) {
				r.Send(id, Replicate{
					Command: m.Command,
				})
			}
			m.Reply(paxi.Reply{
				Command: m.Command,
				Value:   v,
			})
		} else {
			go r.Forward(id, m)
		}
	}
}

func (r *Replica) HandleReplicate(m Replicate) {
	r.Node.Execute(m.Command)
}
