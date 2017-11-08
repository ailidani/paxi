-------------------------------- MODULE wpaxos-------------------------------

EXTENDS Integers, Sequences, FiniteSets, TLC

-----------------------------------------------------------------------------

(***************************************************************************)
(* N number of nodes in each zone                                          *)
(***************************************************************************)
CONSTANT N

(***************************************************************************)
(* M number of zones                                                       *)
(***************************************************************************)
CONSTANT M

CONSTANTS Commands, Txns, Objects, Access(_)

Zones == 1..M
Nodes == Zones \X 1..N
Leaders == Zones \X 1
Q1 == \A Q \in SUBSET Nodes : /\ Cardinality(Q) = M
                              /\ \A i,j \in Q : i[1] # j[1]
Q2 == \A Q \in SUBSET Nodes : /\ Cardinality(Q) = N
                              /\ \A i,j \in Q : i[1] = j[1]

ASSUME IsFiniteSet(Nodes)

(***************************************************************************)
(* Quorum assumption                                                       *)
(***************************************************************************)
ASSUME QuorumAssumption == /\ \A q \in Q1 : q \subseteq Nodes
                           /\ \A q \in Q2 : q \subseteq Nodes
                           /\ \A q1 \in Q1 : \A q2 \in Q2 : q1 \cap q2 # {}

(***************************************************************************)
(* Command and Txn access assumption                                       *)
(***************************************************************************)
ASSUME AccessAssumption == /\ \A c \in Commands : Access(c) \in Objects
                           /\ \A t \in Txns : Access(t) \in SUBSET Objects

(***************************************************************************)
(* Ballot and Slot definition                                              *)
(***************************************************************************)
Ballots == Nat
Slots == Objects \X (1..Cardinality(Commands))

(***************************************************************************)
(* All possible protocol messages:                                         *)
(***************************************************************************)

Messages ==     [type:{"prepare"}, n:Nodes, o:Objects, b:Ballots]
         \union [type:{"promise"}, n:Nodes, o:Objects, b:Ballots, s:Slots]
         \union [type:{"accept"}, n:Nodes, b:Ballots, s:Slots, c:Commands]
         \union [type:{"accepted"}, n:Nodes, o:Objects, b:Ballots, s:Slots]
         \union [type:{"commit"}, n:Nodes, o:Objects, b:Ballots, s:Slots, c:Commands]

(***************************************************************************
(* wpaxos pluscal *)
--algorithm wpaxos {

    variable msgs = {};
             proposed = {}; \* c \in Commands

    define {
        Q1Satisfied(b) == \E Q \in Q1 : \A n \in Q : \E m \in msgs : m.type = "ack" /\ m.n = n /\ m.b = b
        Q2Satisfied(b) == \E Q \in Q2 : \A n \in Q : \E m \in msgs : m.type = "ack" /\ m.n = n /\ m.b = b
        
        MaxV(s, S) == CHOOSE i \in S : i.s = s /\ \A j \in S : i.b >= j.b

        Committed(o, s, vote) == \E c \in Commands : \E b \in Ballots : \E q2 \in Q2 : \A n \in q2 : vote[n][s][b] = c
\*        NextSlot(o, vote) == LET committed == {s \in Slots : Committed(o, s, vote)}
\*                             IN IF (committed # {}) THEN SetMax(committed) + 1 ELSE 0
    }
    
    macro Send(m) {
        msgs := msgs \union {m};
    }

    macro HandlePrepare() {
        with (msg \in msgs) {
            when msg.type = "prepare" /\ msg.b > ballots[msg.o];
            ballots[msg.o] := msg.b;
            if (msg.o \in own) { own := own \ {msg.o} };
            Send([type |-> "promise", n |-> self, o |-> msg.o, b |-> msg.b, s |-> slots[msg.o]]);
        }
    }

    macro HandlePromise() {
        with (msg \in msgs) {
            when msg.type = "promise" /\ msg.b = ballots[msg.o];
            if (Q1Satisfied(ballots[msg.o])) {
                own := own \union {msg.o};
                slots[msg.o] := slots[msg.o] + 1;
                Send([type |-> "accept", n |-> self, b |-> ballots[msg.o], s |-> slots[msg.o], c |-> MaxV(slots[msg.o], accepted[msg.o])]);
            }
        }
    }

    macro HandleAccept() {
        with (msg \in msgs) {
            when msg.type = "accept" /\ msg.b >= ballots[Access(msg.c)];
            ballots[Access(msg.c)] := msg.b;
            accepted[Access(msg.c)] := @ \union {[s |-> msg.s, b |-> msg.b, c |-> msg.c]};
            Send([type |-> "accepted", n |-> self, o |-> Access(msg.c), b |-> msg.b, s |-> msg.s]);
        }
    }

    macro HandleAccepted() {
        with (msg \in msgs) {
            when msg.type = "accepted" /\ msg.b = ballots[msg.o];
            if (Q2Satisfied(ballots[msg.o])) {
                decided[msg.o][msg.s] := <<msg.b, msg>>;
            }
        }
    }

    (************************)

    macro propose() {
        with (c \in (Commands \ proposed)) {
            proposed := proposed \union {c};
            if (Access(c) \notin own) {
                ballots[Access(c)] := @ + 1;
                Send([type |-> "prepare", n |-> self, o |-> Access(c), b |-> ballots[Access(c)]]);
            }
            else {
                slots[Access(c)] := @ + 1;
                log[Access(c)][slots[Access(c)]] := [cmd |-> c, commit |-> FALSE];
                Send([type |-> "accept", n |-> self, o |-> Access(c), b |-> ballots[Access(c)]]);
            }
        }
    }

    fair process (node \in Nodes)
    variables ballots = [o \in Objects |-> 0]; \* highest ballot numbers
              slots = [o \in Objects |-> 0]; \* highest slot numbers
              log = [o \in Objects |-> [s \in Slots |-> <<>>]];
              own = {}; \* objects I own
              accepted = [o \in Objects |-> {}]; \* [s -> Slots, b -> Ballots, c -> Commands]
              decided = [o \in Objects |-> [s \in Slots |-> <<>>]]; \* <<b, c>>
    {
        start: while(TRUE) {
            either propose: propose();
            or HandlePrepare: HandlePrepare();
            or HandlePromise: HandlePromise();
            or HandleAccept: HandleAccept();
            or HandleAccepted: HandleAccepted();
        }
    }
}

***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES msgs, proposed, pc

(* define statement *)
Q1Satisfied(b) == \E Q \in Q1 : \A n \in Q : \E m \in msgs : m.type = "ack" /\ m.n = n /\ m.b = b
Q2Satisfied(b) == \E Q \in Q2 : \A n \in Q : \E m \in msgs : m.type = "ack" /\ m.n = n /\ m.b = b

MaxV(s, S) == CHOOSE i \in S : i.s = s /\ \A j \in S : i.b >= j.b

Committed(o, s, vote) == \E c \in Commands : \E b \in Ballots : \E q2 \in Q2 : \A n \in q2 : vote[n][s][b] = c

VARIABLES ballots, slots, log, own, accepted, decided

vars == << msgs, proposed, pc, ballots, slots, log, own, accepted, decided >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ msgs = {}
        /\ proposed = {}
        (* Process node *)
        /\ ballots = [self \in Nodes |-> [o \in Objects |-> 0]]
        /\ slots = [self \in Nodes |-> [o \in Objects |-> 0]]
        /\ log = [self \in Nodes |-> [o \in Objects |-> [s \in Slots |-> <<>>]]]
        /\ own = [self \in Nodes |-> {}]
        /\ accepted = [self \in Nodes |-> [o \in Objects |-> {}]]
        /\ decided = [self \in Nodes |-> [o \in Objects |-> [s \in Slots |-> <<>>]]]
        /\ pc = [self \in ProcSet |-> "start"]

start(self) == /\ pc[self] = "start"
               /\ \/ /\ pc' = [pc EXCEPT ![self] = "propose"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "HandlePrepare"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "HandlePromise"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "HandleAccept"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "HandleAccepted"]
               /\ UNCHANGED << msgs, proposed, ballots, slots, log, own, 
                               accepted, decided >>

propose(self) == /\ pc[self] = "propose"
                 /\ \E c \in (Commands \ proposed):
                      /\ proposed' = (proposed \union {c})
                      /\ IF Access(c) \notin own[self]
                            THEN /\ ballots' = [ballots EXCEPT ![self][Access(c)] = @ + 1]
                                 /\ msgs' = (msgs \union {([type |-> "prepare", n |-> self, o |-> Access(c), b |-> ballots'[self][Access(c)]])})
                                 /\ UNCHANGED << slots, log >>
                            ELSE /\ slots' = [slots EXCEPT ![self][Access(c)] = @ + 1]
                                 /\ log' = [log EXCEPT ![self][Access(c)][slots'[self][Access(c)]] = [cmd |-> c, commit |-> FALSE]]
                                 /\ msgs' = (msgs \union {([type |-> "accept", n |-> self, o |-> Access(c), b |-> ballots[self][Access(c)]])})
                                 /\ UNCHANGED ballots
                 /\ pc' = [pc EXCEPT ![self] = "start"]
                 /\ UNCHANGED << own, accepted, decided >>

HandlePrepare(self) == /\ pc[self] = "HandlePrepare"
                       /\ \E msg \in msgs:
                            /\ msg.type = "prepare" /\ msg.b > ballots[self][msg.o]
                            /\ ballots' = [ballots EXCEPT ![self][msg.o] = msg.b]
                            /\ IF msg.o \in own[self]
                                  THEN /\ own' = [own EXCEPT ![self] = own[self] \ {msg.o}]
                                  ELSE /\ TRUE
                                       /\ own' = own
                            /\ msgs' = (msgs \union {([type |-> "promise", n |-> self, o |-> msg.o, b |-> msg.b, s |-> slots[self][msg.o]])})
                       /\ pc' = [pc EXCEPT ![self] = "start"]
                       /\ UNCHANGED << proposed, slots, log, accepted, decided >>

HandlePromise(self) == /\ pc[self] = "HandlePromise"
                       /\ \E msg \in msgs:
                            /\ msg.type = "promise" /\ msg.b = ballots[self][msg.o]
                            /\ IF Q1Satisfied(ballots[self][msg.o])
                                  THEN /\ own' = [own EXCEPT ![self] = own[self] \union {msg.o}]
                                       /\ slots' = [slots EXCEPT ![self][msg.o] = slots[self][msg.o] + 1]
                                       /\ msgs' = (msgs \union {([type |-> "accept", n |-> self, b |-> ballots[self][msg.o], s |-> slots'[self][msg.o], c |-> MaxV(slots'[self][msg.o], accepted[self][msg.o])])})
                                  ELSE /\ TRUE
                                       /\ UNCHANGED << msgs, slots, own >>
                       /\ pc' = [pc EXCEPT ![self] = "start"]
                       /\ UNCHANGED << proposed, ballots, log, accepted, 
                                       decided >>

HandleAccept(self) == /\ pc[self] = "HandleAccept"
                      /\ \E msg \in msgs:
                           /\ msg.type = "accept" /\ msg.b >= ballots[self][Access(msg.c)]
                           /\ ballots' = [ballots EXCEPT ![self][Access(msg.c)] = msg.b]
                           /\ accepted' = [accepted EXCEPT ![self][Access(msg.c)] = @ \union {[s |-> msg.s, b |-> msg.b, c |-> msg.c]}]
                           /\ msgs' = (msgs \union {([type |-> "accepted", n |-> self, o |-> Access(msg.c), b |-> msg.b, s |-> msg.s])})
                      /\ pc' = [pc EXCEPT ![self] = "start"]
                      /\ UNCHANGED << proposed, slots, log, own, decided >>

HandleAccepted(self) == /\ pc[self] = "HandleAccepted"
                        /\ \E msg \in msgs:
                             /\ msg.type = "accepted" /\ msg.b = ballots[self][msg.o]
                             /\ IF Q2Satisfied(ballots[self][msg.o])
                                   THEN /\ decided' = [decided EXCEPT ![self][msg.o][msg.s] = <<msg.b, msg>>]
                                   ELSE /\ TRUE
                                        /\ UNCHANGED decided
                        /\ pc' = [pc EXCEPT ![self] = "start"]
                        /\ UNCHANGED << msgs, proposed, ballots, slots, log, 
                                        own, accepted >>

node(self) == start(self) \/ propose(self) \/ HandlePrepare(self)
                 \/ HandlePromise(self) \/ HandleAccept(self)
                 \/ HandleAccepted(self)

Next == (\E self \in Nodes: node(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Nodes : WF_vars(node(self))

\* END TRANSLATION


-----------------------------------------------------------------------------

(***************************************************************************)
(* Adding a key-value mapping (kv[1] is the key, kv[2] the value) to a map *)
(***************************************************************************)
f ++ kv == [x \in DOMAIN f \union {kv[1]} |-> IF x = kv[1] THEN kv[2] ELSE f[x]]

TypeOK == /\ msgs \subseteq Messages

=============================================================================
