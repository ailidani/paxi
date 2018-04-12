package wankeeper

import "github.com/ailidani/paxi"

type master struct {
	*leader

	leaders map[int]paxi.ID
	pending map[paxi.Key][]*paxi.Request
	policy  map[paxi.Key]paxi.Policy
}

func newMaster(l *leader) *master {
	m := &master{
		leader:  l,
		leaders: make(map[int]paxi.ID),
		pending: make(map[paxi.Key][]*paxi.Request),
		policy:  make(map[paxi.Key]paxi.Policy),
	}
	m.leader.Replica.tokens.master = true
	return m
}

func (m *master) lead(r *paxi.Request) {
	// when it reach here, master don't have token
	key := r.Command.Key
	id := m.tokens.get(key)
	// add to pending request
	if m.pending[key] == nil {
		m.pending[key] = make([]*paxi.Request, 0)
		m.Send(id, Revoke{key})
	}
	m.pending[key] = append(m.pending[key], r)
}

func (m *master) handleToken(t Token) {
	// received revoked token
	if len(m.pending[t.Key]) > 0 {
		m.leader.lead(m.pending[t.Key]...)
		m.pending[t.Key] = nil
	}
}

func (m *master) handleCommit(c Commit) {
	k := c.Command.Key
	if c.Ballot.ID() == m.ID() && m.tokens.contains(k) {
		if m.policy[k] == nil {
			m.policy[k] = paxi.NewPolicy()
		}
		id := m.policy[k].Hit(c.Command.ClientID)
		if id != "" && id != m.ID() {
			m.leader.Replica.tokens.set(k, id)
			defer m.Send(id, Token{k})
		}
	}
	for _, id := range m.leaders {
		if id == c.Ballot.ID() {
			continue
		}
		m.Send(id, c)
	}
}

func (m *master) handleNewLeader(n NewLeader) {
	m.leaders[n.Ballot.ID().Zone()] = n.Ballot.ID()
}
