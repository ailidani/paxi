package epaxos

import (
	"encoding/gob"
	. "paxi"
	"paxi/glog"
)

const HT_INIT_SIZE = 200000
const BF_K = 4

var bf_PT uint32

type Replica struct {
	*Node
	N                int // total number of replicas
	InstanceSpace    map[ID][]*Instance
	crtInstance      map[ID]int
	CommittedUpTo    map[ID]int
	ExecutedUpTo     map[ID]int
	conflicts        map[ID]map[Key]int
	maxSeqPerKey     map[Key]int
	maxSeq           int
	latestCPReplica  ID
	latestCPInstance int

	Shutdown           bool
	instancesToRecover chan *instanceId
}

type Instance struct {
	cmds    []Command
	ballot  int
	status  int8
	seq     int
	deps    map[ID]int
	lb      *LeaderBookkeeping
	index   int
	lowlink int
	bfilter *Bloomfilter
}

type instanceId struct {
	replica  ID
	instance int
}

type RecoveryInstance struct {
	cmds            []Command
	status          int8
	seq             int
	deps            map[ID]int
	preAcceptCount  int
	leaderResponded bool
}

type LeaderBookkeeping struct {
	proposals         []Request
	maxRecvBallot     int
	prepareQuorum     *Quorum
	allEqual          bool
	preAcceptQuorum   *Quorum
	acceptQuorum      *Quorum
	nacks             int
	originalDeps      map[ID]int
	committedDeps     map[ID]int
	recoveryInst      *RecoveryInstance
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool //???
	tpaOKs            int    //???
}

func NewLeaderBookkeeping(proposals []Request, deps map[ID]int) *LeaderBookkeeping {
	lb := &LeaderBookkeeping{
		proposals:       proposals,
		maxRecvBallot:   0,
		prepareQuorum:   NewQuorum(),
		allEqual:        true,
		preAcceptQuorum: NewQuorum(),
		acceptQuorum:    NewQuorum(),
		nacks:           0,
		originalDeps:    deps,
		committedDeps:   make(map[ID]int),
	}
	for id := range deps {
		lb.committedDeps[id] = -1
	}
	return lb
}

func NewReplica(config *Config) *Replica {
	gob.Register(Prepare{})
	gob.Register(PrepareReply{})
	gob.Register(PreAccept{})
	gob.Register(PreAcceptReply{})
	gob.Register(PreAcceptOK{})
	gob.Register(Accept{})
	gob.Register(AcceptReply{})
	gob.Register(Commit{})
	gob.Register(CommitShort{})
	gob.Register(TryPreAccept{})
	gob.Register(TryPreAcceptReply{})

	N := len(config.Addrs)

	r := &Replica{
		N:                  N,
		Node:               NewNode(config),
		InstanceSpace:      make(map[ID][]*Instance, N),
		crtInstance:        make(map[ID]int, N),
		CommittedUpTo:      make(map[ID]int, N),
		ExecutedUpTo:       make(map[ID]int, N),
		conflicts:          make(map[ID]map[Key]int, N),
		maxSeqPerKey:       make(map[Key]int),
		maxSeq:             0,
		latestCPReplica:    0,
		latestCPInstance:   -1,
		instancesToRecover: make(chan *instanceId, CHAN_BUFFER_SIZE),
	}

	for id, _ := range r.Peers {
		r.InstanceSpace[id] = make([]*Instance, 2*1024*1024)
		r.crtInstance[id] = 0
		r.ExecutedUpTo[id] = -1
		r.conflicts[id] = make(map[Key]int, HT_INIT_SIZE)
	}

	return r
}

/***********************************
     Main event processing loop
************************************/

func (r *Replica) Run() {
	go r.messageLoop()
	r.Node.Run()
}

func (r *Replica) messageLoop() {
	for !r.Shutdown {
		select {

		case proposal := <-r.RequestChan:
			glog.V(2).Infof("Replica %s received %v\n", r.ID, proposal)
			r.handleProposal(proposal)
			break

		case iid := <-r.instancesToRecover:
			r.startRecoveryForInstance(iid.replica, iid.instance)

		case msg := <-r.MessageChan:
			switch msg := msg.(type) {
			case Prepare:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.LeaderId, msg, r.ID)
				r.handlePrepare(&msg)
				break

			case PreAccept:
				//glog.V(2).Infof("Received PreAccept for instance %d.%d\n", msg.LeaderId, msg.Instance)
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.LeaderId, msg, r.ID)
				r.handlePreAccept(&msg)
				break

			case Accept:
				glog.V(2).Infof("Received Accept for instance %d.%d\n", msg.LeaderId, msg.Instance)
				r.handleAccept(&msg)
				break

			case Commit:
				glog.V(2).Infof("Received Commit for instance %d.%d\n", msg.LeaderId, msg.Instance)
				r.handleCommit(&msg)
				break

			case CommitShort:
				glog.V(2).Infof("Received CommitShort for instance %d.%d\n", msg.LeaderId, msg.Instance)
				r.handleCommitShort(&msg)
				break

			case PrepareReply:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.Replica, msg, r.ID)
				r.handlePrepareReply(&msg)
				break

			case PreAcceptReply:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.Replica, msg, r.ID)
				r.handlePreAcceptReply(&msg)
				break

			case PreAcceptOK:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.Replica, msg, r.ID)
				r.handlePreAcceptOK(&msg)
				break

			case AcceptReply:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.Replica, msg, r.ID)
				r.handleAcceptReply(&msg)
				break

			case TryPreAccept:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.LeaderId, msg, r.ID)
				r.handleTryPreAccept(&msg)
				break

			case TryPreAcceptReply:
				glog.V(2).Infof("Replica %s ===[%v]===>>> Replica %s\n", msg.Replica, msg, r.ID)
				r.handleTryPreAcceptReply(&msg)
				break

			}
		}
	}
}

/********************************
         Helper Functions
*********************************/

var conflicted, weird, slow, happy int

func (r *Replica) clearHashtables() {
	for id := range r.Peers {
		r.conflicts[id] = make(map[Key]int, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(id ID) {
	nextSeq := r.CommittedUpTo[id] + 1
	nextIns := r.InstanceSpace[id][nextSeq]
	for nextIns != nil && (nextIns.status == COMMITTED || nextIns.status == EXECUTED) {
		r.CommittedUpTo[id] = nextSeq
		nextSeq = nextSeq + 1
		nextIns = r.InstanceSpace[id][nextSeq]
	}
}

func (r *Replica) updateConflicts(cmds []Command, id ID, instance int, seq int) {
	for _, cmd := range cmds {
		key := cmd.Key
		if d, present := r.conflicts[id][key]; present {
			if d < instance {
				r.conflicts[id][key] = instance
			}
		} else {
			r.conflicts[id][key] = instance
		}
		if s, present := r.maxSeqPerKey[key]; present {
			if s < seq {
				r.maxSeqPerKey[key] = seq
			}
		} else {
			r.maxSeqPerKey[key] = seq
		}
	}
}

func (r *Replica) updateAttributes(cmds []Command, seq int, deps map[ID]int, replica ID, instance int) (int, map[ID]int, bool) {
	changed := false
	for id := range r.Peers {
		if r.ID != replica && id == replica {
			continue
		}
		for _, cmd := range cmds {
			if d, present := r.conflicts[id][cmd.Key]; present {
				if d > deps[id] {
					deps[id] = d
					if seq <= r.InstanceSpace[id][d].seq {
						seq = r.InstanceSpace[id][d].seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for _, cmd := range cmds {
		if s, present := r.maxSeqPerKey[cmd.Key]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}
	return seq, deps, changed
}

func (r *Replica) mergeAttributes(seq1 int, deps1 map[ID]int, seq2 int, deps2 map[ID]int) (int, map[ID]int, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for id := range r.Peers {
		if id == r.ID {
			continue
		}
		if deps1[id] != deps2[id] {
			equal = false
			if deps2[id] > deps1[id] {
				deps1[id] = deps2[id]
			}
		}
	}
	return seq1, deps1, equal
}

func equal(deps1, deps2 map[ID]int) bool {
	for id := range deps1 {
		if deps1[id] != deps2[id] {
			return false
		}
	}
	return true
}

func bfFromCommands(cmds []Command) *Bloomfilter {
	if cmds == nil {
		return nil
	}
	bf := NewPowTwo(bf_PT, BF_K)
	for i := 0; i < len(cmds); i++ {
		bf.AddUint64(uint64(cmds[i].Key))
	}
	return bf
}

/* Ballot helper functions */

func (r *Replica) makeUniqueBallot(ballot int) int {
	return (ballot << 4) | int(r.ID)
}

func (r *Replica) makeBallotLargerThan(ballot int) int {
	return r.makeUniqueBallot((ballot >> 4) + 1)
}

func isInitialBallot(ballot int) bool {
	return (ballot >> 4) == 0
}

func replicaIDFromBallot(ballot int) ID {
	return ID(ballot & 15)
}

/********************************
              Phase 1
*********************************/

func (r *Replica) handleProposal(msg Request) {
	instance := r.crtInstance[r.ID]
	r.crtInstance[r.ID]++

	glog.V(2).Infof("Starting instance %d\n", instance)

	cmds := []Command{msg.Command}

	r.startPhase1(r.ID, instance, 0, []Request{msg}, cmds)
}

func (r *Replica) startPhase1(replica ID, instance int, ballot int, proposals []Request, cmds []Command) {
	seq := 0
	deps := make(map[ID]int)
	for id := range r.Peers {
		deps[id] = -1
	}

	seq, deps, _ = r.updateAttributes(cmds, seq, deps, r.ID, instance)

	r.InstanceSpace[r.ID][instance] = &Instance{
		cmds:   cmds,
		ballot: ballot,
		status: PREACCEPTED,
		seq:    seq,
		deps:   deps,
		lb:     NewLeaderBookkeeping(proposals, deps),
	}
	r.InstanceSpace[r.ID][instance].lb.preAcceptQuorum.ACK(r.ID)
	r.InstanceSpace[r.ID][instance].lb.prepareQuorum.ACK(r.ID)
	r.InstanceSpace[r.ID][instance].lb.acceptQuorum.ACK(r.ID)

	r.updateConflicts(cmds, r.ID, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq + 1
	}

	pa := &PreAccept{
		LeaderId: r.ID,
		Replica:  r.ID,
		Instance: instance,
		Ballot:   ballot,
		Command:  cmds,
		Seq:      seq,
		Deps:     deps,
	}
	r.Broadcast(pa)
	// do checkpoint
}

func (r *Replica) handlePreAccept(msg *PreAccept) {
	inst := r.InstanceSpace[msg.LeaderId][msg.Instance]

	if msg.Seq >= r.maxSeq {
		r.maxSeq = msg.Seq + 1
	}

	if inst != nil && (inst.status == COMMITTED || inst.status == ACCEPTED) {
		if inst.cmds == nil {
			r.InstanceSpace[msg.LeaderId][msg.Instance].cmds = msg.Command
			r.updateConflicts(msg.Command, msg.Replica, msg.Instance, msg.Seq)
		}
		return
	}

	if msg.Instance >= r.crtInstance[msg.Replica] {
		r.crtInstance[msg.Replica] = msg.Instance + 1
	}

	//update attributes for command
	seq, deps, changed := r.updateAttributes(msg.Command, msg.Seq, msg.Deps, msg.Replica, msg.Instance)
	uncommittedDeps := false
	for id := range r.Peers {
		if deps[id] > r.CommittedUpTo[id] {
			uncommittedDeps = true
			break
		}
	}
	status := PREACCEPTED_EQ
	if changed {
		status = PREACCEPTED
	}

	if inst != nil {
		if msg.Ballot < inst.ballot {
			reply := &PreAcceptReply{
				Replica:       msg.Replica,
				Instance:      msg.Instance,
				OK:            false,
				Ballot:        inst.ballot,
				Seq:           inst.seq,
				Deps:          inst.deps,
				CommittedDeps: r.CommittedUpTo,
			}
			r.Send(msg.LeaderId, reply)
			return
		} else {
			inst.cmds = msg.Command
			inst.seq = seq
			inst.deps = deps
			inst.ballot = msg.Ballot
			inst.status = status
		}
	} else {
		r.InstanceSpace[msg.Replica][msg.Instance] = &Instance{
			cmds:   msg.Command,
			ballot: msg.Ballot,
			status: status,
			seq:    seq,
			deps:   deps,
		}
	}

	r.updateConflicts(msg.Command, msg.Replica, msg.Instance, msg.Seq)

	if len(msg.Command) == 0 {
		//checkpoint
		//update latest checkpoint info
		r.latestCPReplica = msg.Replica
		r.latestCPInstance = msg.Instance

		//discard dependency hashtables
		r.clearHashtables()
	}

	if changed || uncommittedDeps || msg.Replica != msg.LeaderId || !isInitialBallot(msg.Ballot) {
		reply := &PreAcceptReply{
			Replica:       msg.Replica,
			Instance:      msg.Instance,
			OK:            true,
			Ballot:        msg.Ballot,
			Seq:           seq,
			Deps:          deps,
			CommittedDeps: r.CommittedUpTo,
		}
		r.Send(msg.LeaderId, reply)
	} else {
		r.Send(msg.LeaderId, &PreAcceptOK{r.ID, msg.Instance})
	}

	glog.V(2).Infof("I've replied to the PreAccept\n")
}

func (r *Replica) handlePreAcceptReply(msg *PreAcceptReply) {
	glog.V(2).Infof("Handling PreAccept reply\n")
	inst := r.InstanceSpace[msg.Replica][msg.Instance]

	if inst.status != PREACCEPTED {
		return
	}
	if inst.ballot != msg.Ballot {
		return
	}
	if !msg.OK {
		inst.lb.nacks++
		if msg.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = msg.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	// inst.lb.preAcceptQuorum.ACK(msg.Replica)
	inst.lb.preAcceptQuorum.ADD()

	var equal bool
	inst.seq, inst.deps, equal = r.mergeAttributes(inst.seq, inst.deps, msg.Seq, msg.Deps)
	if (r.N <= 3 && !r.Thrifty) || inst.lb.preAcceptQuorum.Size() > 1 {
		inst.lb.allEqual = inst.lb.allEqual && equal
		if !equal {
			conflicted++
		}
	}

	allCommitted := true
	for id := range r.Peers {
		if inst.lb.committedDeps[id] < msg.CommittedDeps[id] {
			inst.lb.committedDeps[id] = msg.CommittedDeps[id]
		}
		if inst.lb.committedDeps[id] < r.CommittedUpTo[id] {
			inst.lb.committedDeps[id] = r.CommittedUpTo[id]
		}
		if inst.lb.committedDeps[id] < inst.deps[id] {
			allCommitted = false
		}
	}

	//can we commit on the fast path?
	if inst.lb.preAcceptQuorum.FastPath() && inst.lb.allEqual && allCommitted && isInitialBallot(inst.ballot) {
		happy++
		glog.V(2).Infof("Fast path for instance %d.%d\n", msg.Replica, msg.Instance)
		r.InstanceSpace[msg.Replica][msg.Instance].status = COMMITTED
		r.updateCommitted(msg.Replica)
		if inst.lb.proposals != nil {
			// give clients the all clear
			for _, p := range inst.lb.proposals {
				reply := Reply{
					OK:        true,
					CommandID: p.CommandID,
					LeaderID:  r.ID,
					ClientID:  p.ClientID,
					Command:   p.Command,
					Timestamp: p.Timestamp,
				}
				r.ReplyChan <- reply
			}
		}
		commit := &Commit{
			r.ID, r.ID, msg.Instance, inst.cmds, inst.seq, inst.deps,
		}
		r.Broadcast(commit)

	} else if inst.lb.preAcceptQuorum.Majority() {
		if !allCommitted {
			weird++
		}
		slow++
		inst.status = ACCEPTED
		accept := &Accept{
			r.ID, r.ID, msg.Instance, inst.ballot, len(inst.cmds), inst.seq, inst.deps,
		}
		r.Broadcast(accept)
	}
	//TODO: take the slow path if messages are slow to arrive
}

func (r *Replica) handlePreAcceptOK(msg *PreAcceptOK) {
	inst := r.InstanceSpace[r.ID][msg.Instance]

	if inst.status != PREACCEPTED {
		// we've moved on, this is a delayed reply
		glog.V(2).Infoln(" we've moved on, this is a delayed reply")
		return
	}

	if !isInitialBallot(inst.ballot) {
		glog.V(2).Infof("is not initial ballot = %v\n", inst.ballot)
		return
	}

	// inst.lb.preAcceptQuorum.ACK(msg.Replica)
	inst.lb.preAcceptQuorum.ADD()

	allCommitted := true
	for id := range r.Peers {
		if inst.lb.committedDeps[id] < inst.lb.originalDeps[id] {
			inst.lb.committedDeps[id] = inst.lb.originalDeps[id]
		}
		if inst.lb.committedDeps[id] < r.CommittedUpTo[id] {
			inst.lb.committedDeps[id] = r.CommittedUpTo[id]
		}
		if inst.lb.committedDeps[id] < inst.deps[id] {
			allCommitted = false
		}
	}

	//can we commit on the fast path?
	if inst.lb.preAcceptQuorum.FastPath() && inst.lb.allEqual && allCommitted && isInitialBallot(inst.ballot) {

		happy++
		r.InstanceSpace[r.ID][msg.Instance].status = COMMITTED
		r.updateCommitted(r.ID)
		if inst.lb.proposals != nil {
			// give clients the all clear
			for _, p := range inst.lb.proposals {
				reply := Reply{
					OK:        true,
					CommandID: p.CommandID,
					LeaderID:  r.ID,
					ClientID:  p.ClientID,
					Command:   p.Command,
					Timestamp: p.Timestamp,
				}
				r.ReplyChan <- reply
			}
		}
		commit := &Commit{
			r.ID, r.ID, msg.Instance, inst.cmds, inst.seq, inst.deps,
		}
		r.Broadcast(commit)
	} else if inst.lb.preAcceptQuorum.Majority() {
		if !allCommitted {
			weird++
		}
		slow++
		inst.status = ACCEPTED
		accept := &Accept{
			r.ID, r.ID, msg.Instance, inst.ballot, len(inst.cmds), inst.seq, inst.deps,
		}
		r.Broadcast(accept)
	}
	//TODO: take the slow path if messages are slow to arrive
}

/********************************
              Phase 2
*********************************/

func (r *Replica) handleAccept(msg *Accept) {
	inst := r.InstanceSpace[msg.LeaderId][msg.Instance]

	if msg.Seq >= r.maxSeq {
		r.maxSeq = msg.Seq + 1
	}

	if inst != nil && (inst.status == COMMITTED || inst.status == EXECUTED) {
		return
	}

	if msg.Instance >= r.crtInstance[msg.LeaderId] {
		r.crtInstance[msg.LeaderId] = msg.Instance + 1
	}

	if inst != nil {
		if msg.Ballot < inst.ballot {
			r.Send(msg.LeaderId, &AcceptReply{msg.Replica, msg.Instance, false, inst.ballot})
			return
		}
		inst.status = ACCEPTED
		inst.seq = msg.Seq
		inst.deps = msg.Deps
	} else {
		r.InstanceSpace[msg.LeaderId][msg.Instance] = &Instance{
			nil,
			msg.Ballot,
			ACCEPTED,
			msg.Seq,
			msg.Deps,
			nil, 0, 0, nil}

		if msg.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = msg.Replica
			r.latestCPInstance = msg.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}

	r.Send(msg.LeaderId, &AcceptReply{msg.Replica, msg.Instance, true, msg.Ballot})
}

func (r *Replica) handleAcceptReply(msg *AcceptReply) {
	inst := r.InstanceSpace[msg.Replica][msg.Instance]

	if inst.status != ACCEPTED {
		return
	}
	if inst.ballot != msg.Ballot {
		return
	}

	if !msg.OK {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if msg.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = msg.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	// inst.lb.acceptQuorum.ACK(msg.Replica)
	inst.lb.acceptQuorum.ADD()

	if inst.lb.acceptQuorum.Majority() {
		r.InstanceSpace[msg.Replica][msg.Instance].status = COMMITTED
		r.updateCommitted(msg.Replica)
		if inst.lb.proposals != nil {
			for _, p := range inst.lb.proposals {
				reply := Reply{
					OK:        true,
					CommandID: p.CommandID,
					LeaderID:  r.ID,
					ClientID:  p.ClientID,
					Command:   p.Command,
					Timestamp: p.Timestamp,
				}
				r.ReplyChan <- reply
			}
		}
	}
}

/********************************
              Commit
*********************************/

func (r *Replica) handleCommit(msg *Commit) {
	inst := r.InstanceSpace[msg.Replica][msg.Instance]

	if msg.Seq >= r.maxSeq {
		r.maxSeq = msg.Seq + 1
	}

	if msg.Instance >= r.crtInstance[msg.Replica] {
		r.crtInstance[msg.Replica] = msg.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.proposals != nil && len(msg.Command) == 0 {
			//someone committed a NO-OP, but we have proposals for this instance
			//try in a different instance
			for _, p := range inst.lb.proposals {
				r.RequestChan <- p
			}
			inst.lb = nil
		}
		inst.seq = msg.Seq
		inst.deps = msg.Deps
		inst.status = COMMITTED
	} else {
		r.InstanceSpace[msg.Replica][msg.Instance] = &Instance{
			msg.Command,
			0,
			COMMITTED,
			msg.Seq,
			msg.Deps,
			nil,
			0,
			0,
			nil}
		r.updateConflicts(msg.Command, msg.Replica, msg.Instance, msg.Seq)

		if len(msg.Command) == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = msg.Replica
			r.latestCPInstance = msg.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(msg.Replica)
}

func (r *Replica) handleCommitShort(commit *CommitShort) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.proposals != nil {
			//try command in a different instance
			for _, p := range inst.lb.proposals {
				r.RequestChan <- p
			}
			inst.lb = nil
		}
		inst.seq = commit.Seq
		inst.deps = commit.Deps
		inst.status = COMMITTED
	} else {
		r.InstanceSpace[commit.Replica][commit.Instance] = &Instance{
			nil,
			0,
			COMMITTED,
			commit.Seq,
			commit.Deps,
			nil, 0, 0, nil}

		if commit.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)
}

/********************************
         RECOVERY ACTIONS
*********************************/

func (r *Replica) startRecoveryForInstance(replica ID, instance int) {
	var nildeps map[ID]int
	if r.InstanceSpace[replica][instance] == nil {
		r.InstanceSpace[replica][instance] = &Instance{nil, 0, NONE, 0, nildeps, nil, 0, 0, nil}
	}

	inst := r.InstanceSpace[replica][instance]
	if inst.lb == nil {
		inst.lb = &LeaderBookkeeping{nil, -1, NewQuorum(), false, NewQuorum(), NewQuorum(), 0, nildeps, nil, nil, true, false, nil, 0}

	} else {
		inst.lb = &LeaderBookkeeping{inst.lb.proposals, -1, NewQuorum(), false, NewQuorum(), NewQuorum(), 0, nildeps, nil, nil, true, false, nil, 0}
	}

	if inst.status == ACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.cmds, inst.status, inst.seq, inst.deps, 0, false}
		inst.lb.maxRecvBallot = inst.ballot
	} else if inst.status >= PREACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.cmds, inst.status, inst.seq, inst.deps, 1, (r.ID == replica)}
	}

	//compute larger ballot
	inst.ballot = r.makeBallotLargerThan(inst.ballot)

	r.Broadcast(&Prepare{r.ID, replica, instance, inst.ballot})
}

func (r *Replica) handlePrepare(prepare *Prepare) {
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *PrepareReply
	var nildeps map[ID]int

	if inst == nil {
		r.InstanceSpace[prepare.Replica][prepare.Instance] = &Instance{
			nil,
			prepare.Ballot,
			NONE,
			0,
			nildeps,
			nil, 0, 0, nil}
		preply = &PrepareReply{
			r.ID,
			prepare.Replica,
			prepare.Instance,
			true,
			-1,
			NONE,
			nil,
			-1,
			nildeps}
	} else {
		ok := true
		if prepare.Ballot < inst.ballot {
			ok = false
		} else {
			inst.ballot = prepare.Ballot
		}
		preply = &PrepareReply{
			r.ID,
			prepare.Replica,
			prepare.Instance,
			ok,
			inst.ballot,
			inst.status,
			inst.cmds,
			inst.seq,
			inst.deps}
	}
	r.Send(prepare.LeaderId, preply)
}

func (r *Replica) handlePrepareReply(preply *PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]

	if inst.lb == nil || !inst.lb.preparing {
		// we've moved on -- these are delayed replies, so just ignore
		// TODO: should replies for non-current ballots be ignored?
		return
	}

	if !preply.OK {
		// TODO: there is probably another active leader, back off and retry later
		inst.lb.nacks++
		return
	}

	//Got an ACK (preply.OK == TRUE)
	// inst.lb.prepareQuorum.ACK(preply.Replica)
	inst.lb.prepareQuorum.ADD()

	if preply.Status == COMMITTED || preply.Status == EXECUTED {
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			preply.Command,
			inst.ballot,
			COMMITTED,
			preply.Seq,
			preply.Deps,
			nil, 0, 0, nil}
		commit := &Commit{r.ID, preply.Replica, preply.Instance, inst.cmds, preply.Seq, preply.Deps}
		r.Broadcast(commit)
		//TODO: check if we should send notifications to clients
		return
	}

	if preply.Status == ACCEPTED {
		if inst.lb.recoveryInst == nil || inst.lb.maxRecvBallot < preply.Ballot {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 0, false}
			inst.lb.maxRecvBallot = preply.Ballot
		}
	}

	if (preply.Status == PREACCEPTED || preply.Status == PREACCEPTED_EQ) &&
		(inst.lb.recoveryInst == nil || inst.lb.recoveryInst.status < ACCEPTED) {
		if inst.lb.recoveryInst == nil {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 1, false}
		} else if preply.Seq == inst.seq && equal(preply.Deps, inst.deps) {
			inst.lb.recoveryInst.preAcceptCount++
		} else if preply.Status == PREACCEPTED_EQ {
			// If we get different ordering attributes from pre-acceptors, we must go with the ones
			// that agreed with the initial command leader (in case we do not use Thrifty).
			// This is safe if we use thrifty, although we can also safely start phase 1 in that case.
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 1, false}
		}
		if preply.AcceptorId == preply.Replica {
			//if the reply is from the initial command leader, then it's safe to restart phase 1
			inst.lb.recoveryInst.leaderResponded = true
			return
		}
	}

	if inst.lb.prepareQuorum.Size() < r.N/2 {
		return
	}

	//Received Prepare replies from a majority

	ir := inst.lb.recoveryInst

	if ir != nil {
		//at least one replica has (pre-)accepted this instance
		if ir.status == ACCEPTED ||
			(!ir.leaderResponded && ir.preAcceptCount >= r.N/2 && (r.Thrifty || ir.status == PREACCEPTED_EQ)) {
			//safe to go to Accept phase
			inst.cmds = ir.cmds
			inst.seq = ir.seq
			inst.deps = ir.deps
			inst.status = ACCEPTED
			inst.lb.preparing = false
			accept := &Accept{r.ID, preply.Replica, preply.Instance, inst.ballot, len(inst.cmds), inst.seq, inst.deps}
			r.Broadcast(accept)
		} else if !ir.leaderResponded && ir.preAcceptCount >= (r.N/2+1)/2 {
			//send TryPreAccepts
			//but first try to pre-accept on the local replica
			inst.lb.preAcceptQuorum = NewQuorum()
			inst.lb.nacks = 0
			inst.lb.possibleQuorum = make([]bool, r.N)
			for q := 0; q < r.N; q++ {
				inst.lb.possibleQuorum[q] = true
			}
			if conf, q, i := r.findPreAcceptConflicts(ir.cmds, preply.Replica, preply.Instance, ir.seq, ir.deps); conf {
				if r.InstanceSpace[q][i].status >= COMMITTED {
					//start Phase1 in the initial leader's instance
					r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.proposals, ir.cmds)
					return
				} else {
					inst.lb.nacks = 1
					inst.lb.possibleQuorum[r.ID] = false
				}
			} else {
				inst.cmds = ir.cmds
				inst.seq = ir.seq
				inst.deps = ir.deps
				inst.status = PREACCEPTED
				inst.lb.preAcceptQuorum = NewQuorum()
				inst.lb.preAcceptQuorum.ACK(r.ID)
			}
			inst.lb.preparing = false
			inst.lb.tryingToPreAccept = true
			try := &TryPreAccept{r.ID, preply.Replica, preply.Instance, inst.ballot, inst.cmds, inst.seq, inst.deps}
			r.Broadcast(try)
		} else {
			//start Phase1 in the initial leader's instance
			inst.lb.preparing = false
			r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.proposals, ir.cmds)
		}
	} else {
		//try to finalize instance by proposing NO-OP
		var noop_deps map[ID]int
		// commands that depended on this instance must look at all previous instances
		noop_deps[preply.Replica] = preply.Instance - 1
		inst.lb.preparing = false
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			nil,
			inst.ballot,
			ACCEPTED,
			0,
			noop_deps,
			inst.lb, 0, 0, nil}
		accept := &Accept{r.ID, preply.Replica, preply.Instance, inst.ballot, 0, 0, noop_deps}
		r.Broadcast(accept)
	}
}

func (r *Replica) handleTryPreAccept(tpa *TryPreAccept) {
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]
	if inst != nil && inst.ballot > tpa.Ballot {
		// ballot number too small
		r.Send(tpa.LeaderId, &TryPreAcceptReply{
			r.ID,
			tpa.Replica,
			tpa.Instance,
			false,
			inst.ballot,
			tpa.Replica,
			tpa.Instance,
			inst.status},
		)
	}
	if conflict, confRep, confInst := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
		// there is a conflict, can't pre-accept
		r.Send(tpa.LeaderId, &TryPreAcceptReply{
			r.ID,
			tpa.Replica,
			tpa.Instance,
			false,
			inst.ballot,
			confRep,
			confInst,
			r.InstanceSpace[confRep][confInst].status},
		)
	} else {
		// can pre-accept
		if tpa.Instance >= r.crtInstance[tpa.Replica] {
			r.crtInstance[tpa.Replica] = tpa.Instance + 1
		}
		if inst != nil {
			inst.cmds = tpa.Command
			inst.deps = tpa.Deps
			inst.seq = tpa.Seq
			inst.status = PREACCEPTED
			inst.ballot = tpa.Ballot
		} else {
			r.InstanceSpace[tpa.Replica][tpa.Instance] = &Instance{
				tpa.Command,
				tpa.Ballot,
				PREACCEPTED,
				tpa.Seq,
				tpa.Deps,
				nil, 0, 0,
				nil}
		}
		r.Send(tpa.LeaderId, &TryPreAcceptReply{r.ID, tpa.Replica, tpa.Instance, true, inst.ballot, 0, 0, 0})
	}
}

func (r *Replica) findPreAcceptConflicts(cmds []Command, replica ID, instance int, seq int, deps map[ID]int) (bool, ID, int) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.cmds) > 0 {
		if inst.status >= ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		// if inst.seq == tpa.Seq && equal(inst.deps, tpa.deps) {
		// 	// already PRE-ACCEPTED, no point looking for conflicts again
		// 	return false, replica, instance
		// }
	}
	for id := range r.Peers {
		for i := r.ExecutedUpTo[id]; i < r.crtInstance[id]; i++ {
			if replica == id && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[id] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.InstanceSpace[id][i]
			if inst == nil || inst.cmds == nil || len(inst.cmds) == 0 {
				continue
			}
			if inst.deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if ConflictBatch(inst.cmds, cmds) {
				if i > deps[id] ||
					(i < deps[id] && inst.seq >= seq && (id != replica || inst.status > PREACCEPTED_EQ)) {
					// this is a conflict
					return true, id, i
				}
			}
		}
	}
	return false, 0, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *TryPreAcceptReply) {
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]
	if inst == nil || inst.lb == nil || !inst.lb.tryingToPreAccept || inst.lb.recoveryInst == nil {
		return
	}

	ir := inst.lb.recoveryInst

	if tpar.OK {
		// inst.lb.preAcceptQuorum.ACK(tpar.Replica)
		inst.lb.preAcceptQuorum.ADD()
		inst.lb.tpaOKs++
		if inst.lb.preAcceptQuorum.Majority() {
			//it's safe to start Accept phase
			inst.cmds = ir.cmds
			inst.seq = ir.seq
			inst.deps = ir.deps
			inst.status = ACCEPTED
			inst.lb.tryingToPreAccept = false
			inst.lb.acceptQuorum = NewQuorum()
			r.Broadcast(&Accept{r.ID, tpar.Replica, tpar.Instance, inst.ballot, len(inst.cmds), inst.seq, inst.deps})
			return
		}
	} else {
		inst.lb.nacks++
		if tpar.Ballot > inst.ballot {
			//TODO: retry with higher ballot
			return
		}
		inst.lb.tpaOKs++
		if tpar.ConflictReplica == tpar.Replica && tpar.ConflictInstance == tpar.Instance {
			//TODO: re-run prepare
			inst.lb.tryingToPreAccept = false
			return
		}
		inst.lb.possibleQuorum[tpar.AcceptorId] = false
		inst.lb.possibleQuorum[tpar.ConflictReplica] = false
		notInQuorum := 0
		for q := 0; q < r.N; q++ {
			if !inst.lb.possibleQuorum[tpar.AcceptorId] {
				notInQuorum++
			}
		}
		if tpar.ConflictStatus >= COMMITTED || notInQuorum > r.N/2 {
			//abandon recovery, restart from phase 1
			inst.lb.tryingToPreAccept = false
			r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.proposals, ir.cmds)
		}
		if notInQuorum == r.N/2 {
			//this is to prevent defer cycles
			if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
				if inst.lb.possibleQuorum[dq] {
					//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
					//abandon recovery, restart from phase 1
					inst.lb.tryingToPreAccept = false
					r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.proposals, ir.cmds)
				}
			}
		}
		if inst.lb.tpaOKs >= r.N/2 {
			//defer recovery and update deferred information
			updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
			inst.lb.tryingToPreAccept = false
		}
	}
}

//helper functions and structures to prevent defer cycles while recovering

var deferMap map[uint64]uint64 = make(map[uint64]uint64)

func updateDeferred(dr ID, di int, r ID, i int) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q ID, i int) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}
