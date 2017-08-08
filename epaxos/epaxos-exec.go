package epaxos

import (
	. "paxi"
	"sort"
	"time"
)

const (
	WHITE int8 = iota
	GRAY
	BLACK
)

type Exec struct {
	r *Replica
}

type SCComponent struct {
	nodes []*Instance
	color int8
}

func (e *Exec) executeCommand(replica ID, instance int32) bool {
	if e.r.InstanceSpace[replica][instance] == nil {
		return false
	}
	inst := e.r.InstanceSpace[replica][instance]
	if inst.status == EXECUTED {
		return true
	}
	if inst.status != COMMITTED {
		return false
	}

	if !e.findSCC(inst) {
		return false
	}

	return true
}

var stack []*Instance = make([]*Instance, 0, 100)

func (e *Exec) findSCC(root *Instance) bool {
	index := 1
	//find SCCs using Tarjan's algorithm
	stack = stack[0:0]
	return e.strongconnect(root, &index)
}

func (e *Exec) strongconnect(v *Instance, index *int) bool {
	v.index = *index
	v.lowlink = *index
	*index = *index + 1

	l := len(stack)
	if l == cap(stack) {
		newSlice := make([]*Instance, l, 2*l)
		copy(newSlice, stack)
		stack = newSlice
	}
	stack = stack[0 : l+1]
	stack[l] = v

	for id := range e.r.Peers {
		inst := v.deps[id]
		for i := e.r.ExecutedUpTo[id] + 1; i <= inst; i++ {
			for e.r.InstanceSpace[id][i] == nil || e.r.InstanceSpace[id][i].cmds == nil || v.cmds == nil {
				time.Sleep(1000 * 1000)
			}
			/*        if !state.Conflict(v.Command, e.r.InstanceSpace[q][i].Command) {
			          continue
			          }
			*/
			if e.r.InstanceSpace[id][i].status == EXECUTED {
				continue
			}
			for e.r.InstanceSpace[id][i].status != COMMITTED {
				time.Sleep(1000 * 1000)
			}
			w := e.r.InstanceSpace[id][i]

			if w.index == 0 {
				//e.strongconnect(w, index)
				if !e.strongconnect(w, index) {
					for j := l; j < len(stack); j++ {
						stack[j].index = 0
					}
					stack = stack[0:l]
					return false
				}
				if w.lowlink < v.lowlink {
					v.lowlink = w.lowlink
				}
			} else { //if e.inStack(w)  //<- probably unnecessary condition, saves a linear search
				if w.index < v.lowlink {
					v.lowlink = w.index
				}
			}
		}
	}

	if v.lowlink == v.index {
		//found SCC
		list := stack[l:len(stack)]

		//execute commands in the increasing order of the Seq field
		sort.Sort(nodeArray(list))
		for _, w := range list {
			for w.cmds == nil {
				time.Sleep(1000 * 1000)
			}
			for idx := 0; idx < len(w.cmds); idx++ {
				// val := w.cmds[idx].Execute(e.r.state)
				// if e.r.Dreply && w.lb != nil && w.lb.requests != nil {
				// 	e.r.ReplyProposeTS(
				// 		&ProposeReplyTS{
				// 			true,
				// 			w.lb.clientProposals[idx].CommandId,
				// 			val,
				// 			w.lb.clientProposals[idx].Timestamp},
				// 		w.lb.clientProposals[idx].Reply)
				// }
			}
			w.status = EXECUTED
		}
		stack = stack[0:l]
	}
	return true
}

func (e *Exec) inStack(w *Instance) bool {
	for _, u := range stack {
		if w == u {
			return true
		}
	}
	return false
}

type nodeArray []*Instance

func (na nodeArray) Len() int {
	return len(na)
}

func (na nodeArray) Less(i, j int) bool {
	return na[i].seq < na[j].seq
}

func (na nodeArray) Swap(i, j int) {
	na[i], na[j] = na[j], na[i]
}
