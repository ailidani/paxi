package wankeeper

import "github.com/ailidani/paxi"

type leader struct {
	Replica

	master   paxi.ID
	active   bool
	ballot   paxi.Ballot     // ballot for local group
	quorum   *paxi.Quorum    // quorum for leader election
	requests []*paxi.Request // pending requests
}

func (l *leader) lead(m ...*paxi.Request) {

}
