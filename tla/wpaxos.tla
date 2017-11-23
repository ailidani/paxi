-------------------------------- MODULE wpaxos-------------------------------

EXTENDS Integers, Sequences, FiniteSets, TLC

-----------------------------------------------------------------------------

(***************************************************************************)
(* N number of nodes in each zone                                          *)
(***************************************************************************)
CONSTANT N

(***************************************************************************)
(* Z number of zones                                                       *)
(***************************************************************************)
CONSTANT Z

(***************************************************************************)
(* Fz to tolerant Fz failure zones                                         *)
(***************************************************************************)
CONSTANT Fz
ASSUME Fz >= 0 /\ Fz < Z
\* K is the majority of each zone
K == N \div 2 + 1

CONSTANTS Objects, NumCommands

(***************************************************************************)
(* Definitions                                                             *)
(***************************************************************************)
Zones == 1..Z
Nodes == Zones \X (1..N)
Leaders == Zones \X {1}

ASSUME IsFiniteSet(Nodes)

vertical == {q \in SUBSET Nodes : /\ Cardinality(q) = K
                                  /\ \A i,j \in q : i[1] = j[1]}

SameZone(q) == /\ Cardinality(q) = K
               /\ \A i,j \in q : i[1] = j[1]

Q1 == {q \in SUBSET Nodes : /\ Cardinality(q) = K * (Z - Fz)
                            /\ Cardinality({i[1] : i \in q}) = Z - Fz
                            /\ Cardinality({z \in SUBSET q : SameZone(z)}) = Z - Fz}
\*                            /\ ~ \E k \in SUBSET q : /\ \A i,j \in k : i[1] = j[1]
\*                                                     /\ Cardinality(k) > K}

Q2 == {q \in SUBSET Nodes : /\ Cardinality(q) = K * (Fz + 1)
                            /\ Cardinality({i[1] : i \in q}) = Fz + 1
                            /\ Cardinality({z \in SUBSET q : SameZone(z)}) = Fz + 1}
\*                            /\ ~ \E k \in SUBSET q : /\ \A i, j \in k : i[1] = j[1]
\*                                                     /\ Cardinality(k) > K}

\*Q1 == {q \in SUBSET Nodes : \A q2 \in Q2 : q \cap q2 # {}}

(***************************************************************************)
(* Quorum assumption                                                       *)
(***************************************************************************)
ASSUME QuorumAssumption == /\ \A q \in Q1 : q \subseteq Nodes
                           /\ \A q \in Q2 : q \subseteq Nodes
                           /\ \A q1 \in Q1 : \A q2 \in Q2 : q1 \cap q2 # {}

(***************************************************************************)
(* Command and Txn access assumption                                       *)
(***************************************************************************)
\*ASSUME AccessAssumption == \A c \in Commands : Access(c) \in Objects

(***************************************************************************)
(* Ballot and Slot definition                                              *)
(***************************************************************************)
Ballots == Nat \X Nodes
Slots == 1..NumCommands

(***************************************************************************)
(* All possible protocol messages:                                         *)
(***************************************************************************)
Messages ==     [type:{"1a"}, n:Nodes, o:Objects, b:Ballots]
         \union [type:{"1b"}, n:Nodes, o:Objects, b:Ballots, s:(Slots \cup {0})]
         \union [type:{"2a"}, n:Nodes, o:Objects, b:Ballots, s:Slots, v:Slots \X Nodes]
         \union [type:{"2b"}, n:Nodes, o:Objects, b:Ballots, s:Slots]
         \union [type:{"3"}, n:Nodes, o:Objects, b:Ballots, s:Slots, v:Slots \X Nodes]

(***************************************************************************
(* wpaxos pluscal *)
--algorithm wpaxos {

    variable msgs = {};
             \* number of commands already proposed
             proposed = 0;

    define {
        Q1Satisfied(b) == \E q \in Q1 : \A n \in q : \E m \in msgs : m.type = "1b" /\ m.n = n /\ m.b = b
        Q2Satisfied(b) == \E q \in Q2 : \A n \in q : \E m \in msgs : m.type = "2b" /\ m.n = n /\ m.b = b
                     
\*        RECURSIVE more(_, _)
\*        more(a, b) == IF a \in Nat /\ b \in Nat THEN a > b
\*                      ELSE \/ Head(a) > Head(b)
\*                           \/ Head(a) = Head(b) /\ more(a[2], b[2])

        more(a, b) == \/ a[1] > b[1]
                      \/ a[1] = b[1] /\ a[2][1] > b[2][1]
                      \/ a[1] = b[1] /\ a[2][1] = b[2][1] /\ a[2][2] > b[2][2]
                           
        a \succ b == more(a, b)
        
        a \succeq b == a \succ b \/ a = b
    }
    
    macro Send(m) { msgs := msgs \union {m}; }

    macro p1a() {
        with (m \in msgs) {
            when m.type = "1a";
            when m.b \succeq ballots[m.o];
            ballots[m.o] := m.b;
            if (m.o \in own) { own := own \ {m.o} };
            Send([type |-> "1b", 
                     n |-> self, 
                     o |-> m.o, 
                     b |-> m.b, 
                     s |-> slots[m.o]]);
        }
    }

    macro p1b() {
        with (m \in msgs) {
            when m.type = "1b";
            when m.b = ballots[m.o];
            if (Q1Satisfied(ballots[m.o])) {
                own := own \union {m.o};
                slots[m.o] := slots[m.o] + 1;
\*                log[m.o][m.s] := [b |-> ballots[m.o], v |-> <<slots[m.o], self>>, commit |-> FALSE];
                log[m.o] := Append(@, [b |-> ballots[m.o], v |-> <<slots[m.o], self>>, commit |-> FALSE]);
                Send([type |-> "2a", 
                         n |-> self, 
                         b |-> ballots[m.o], 
                         s |-> slots[m.o], 
                         o |-> m.o, 
                         v |-> <<slots[m.o], self>>]);
            }
        }
    }

    macro p2a() {
        with (m \in msgs) {
            when m.type = "2a";
            when m.b \succeq ballots[m.o];
            ballots[m.o] := m.b;
            log[m.o][m.s] := [b |-> m.b, v |-> m.v, commit |-> FALSE];
            Send([type |-> "2b", 
                     n |-> self, 
                     o |-> m.o, 
                     b |-> m.b, 
                     s |-> m.s]);
        }
    }

    macro p2b() {
        with (m \in msgs) {
            when m.type = "2b";
            when m.b = ballots[m.o];
            if (Q2Satisfied(ballots[m.o])) {
                log[m.o][m.s].commit := TRUE;
                Send([type |-> "3", 
                         n |-> self, 
                         o |-> m.o, 
                         b |-> ballots[m.o], 
                         s |-> m.s]);
            }
        }
    }
    
    macro p3() {
        with (m \in msgs) {
            when m.type = "3";
            log[m.o][m.s].commit := TRUE;
        }
    }
    
    macro propose() {
        when proposed <= NumCommands;
        proposed := proposed + 1;
        with (o \in Objects) {
            if (o \notin own) {
                ballots[o] := <<ballots[o][1] + 1, self>>;
                Send([type |-> "1a", 
                         n |-> self, 
                         o |-> o, 
                         b |-> ballots[o]]);
            }
            else {
                slots[o] := slots[o] + 1;
                log[o] := Append(@, [b |-> ballots[o], v |-> <<slots[o], self>>, commit |-> FALSE]);
\*                log[o][slots[o]] := [b |-> ballots[o], 
\*                                     v |-> <<slots[o], self>>, 
\*                                commit |-> FALSE];
                Send([type |-> "2a", 
                         n |-> self, 
                         o |-> o, 
                         b |-> ballots[o], 
                         s |-> Len(log[o]), 
                         v |-> <<slots[o], self>>]);
            }
        }
    }

    process (node \in Nodes)
    variables \* highest ballot numbers
              ballots = [o \in Objects |-> <<0, self>>];
              \* highest slot numbers
              slots = [o \in Objects |-> 0];
              \* log is a sequence of records [b -> Ballots, v -> Value, commit -> {TRUE, FALSE}]
              log = [o \in Objects |-> [s \in Slots |-> {}]];
              \* objects I own
              own = {};
    {
        start: while(TRUE) {
            either propose: propose();
            or p1a: p1a();
            or p1b: p1b();
            or p2a: p2a();
            or p2b: p2b();
            or p3: p3();
        }
    }
}
***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES msgs, proposed, pc

(* define statement *)
Q1Satisfied(b) == \E q \in Q1 : \A n \in q : \E m \in msgs : m.type = "1b" /\ m.n = n /\ m.b = b
Q2Satisfied(b) == \E q \in Q2 : \A n \in q : \E m \in msgs : m.type = "2b" /\ m.n = n /\ m.b = b






more(a, b) == \/ a[1] > b[1]
              \/ a[1] = b[1] /\ a[2][1] > b[2][1]
              \/ a[1] = b[1] /\ a[2][1] = b[2][1] /\ a[2][2] > b[2][2]

a \succ b == more(a, b)

a \succeq b == a \succ b \/ a = b

VARIABLES ballots, slots, log, own

vars == << msgs, proposed, pc, ballots, slots, log, own >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ msgs = {}
        /\ proposed = 0
        (* Process node *)
        /\ ballots = [self \in Nodes |-> [o \in Objects |-> <<0, self>>]]
        /\ slots = [self \in Nodes |-> [o \in Objects |-> 0]]
        /\ log = [self \in Nodes |-> [o \in Objects |-> <<>>]]
        /\ own = [self \in Nodes |-> {}]
        /\ pc = [self \in ProcSet |-> "start"]

start(self) == /\ pc[self] = "start"
               /\ \/ /\ pc' = [pc EXCEPT ![self] = "propose"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p1a"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p1b"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p2a"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p2b"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p3"]
               /\ UNCHANGED << msgs, proposed, ballots, slots, log, own >>

propose(self) == /\ pc[self] = "propose"
                 /\ proposed <= NumCommands
                 /\ proposed' = proposed + 1
                 /\ \E o \in Objects:
                      IF o \notin own[self]
                         THEN /\ ballots' = [ballots EXCEPT ![self][o] = <<ballots[self][o][1] + 1, self>>]
                              /\ msgs' = (msgs \union {([type |-> "1a",
                                                            n |-> self,
                                                            o |-> o,
                                                            b |-> ballots'[self][o]])})
                              /\ UNCHANGED << slots, log >>
                         ELSE /\ slots' = [slots EXCEPT ![self][o] = slots[self][o] + 1]
                              /\ log' = [log EXCEPT ![self][o] = Append(@, [b |-> ballots[self][o], v |-> <<slots'[self][o], self>>, commit |-> FALSE])]
                              /\ msgs' = (msgs \union {([type |-> "2a",
                                                            n |-> self,
                                                            o |-> o,
                                                            b |-> ballots[self][o],
                                                            s |-> Len(log'[self][o]),
                                                            v |-> <<slots'[self][o], self>>])})
                              /\ UNCHANGED ballots
                 /\ pc' = [pc EXCEPT ![self] = "start"]
                 /\ own' = own

p1a(self) == /\ pc[self] = "p1a"
             /\ \E m \in msgs:
                  /\ m.type = "1a"
                  /\ m.b \succeq ballots[self][m.o]
                  /\ ballots' = [ballots EXCEPT ![self][m.o] = m.b]
                  /\ IF m.o \in own[self]
                        THEN /\ own' = [own EXCEPT ![self] = own[self] \ {m.o}]
                        ELSE /\ TRUE
                             /\ own' = own
                  /\ msgs' = (msgs \union {([type |-> "1b",
                                                n |-> self,
                                                o |-> m.o,
                                                b |-> m.b,
                                                s |-> slots[self][m.o]])})
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED << proposed, slots, log >>

p1b(self) == /\ pc[self] = "p1b"
             /\ \E m \in msgs:
                  /\ m.type = "1b"
                  /\ m.b = ballots[self][m.o]
                  /\ IF Q1Satisfied(ballots[self][m.o])
                        THEN /\ own' = [own EXCEPT ![self] = own[self] \union {m.o}]
                             /\ slots' = [slots EXCEPT ![self][m.o] = slots[self][m.o] + 1]
                             /\ log' = [log EXCEPT ![self][m.o] = Append(@, [b |-> ballots[self][m.o], v |-> <<slots'[self][m.o], self>>, commit |-> FALSE])]
                             /\ msgs' = (msgs \union {([type |-> "2a",
                                                           n |-> self,
                                                           b |-> ballots[self][m.o],
                                                           s |-> slots'[self][m.o],
                                                           o |-> m.o,
                                                           v |-> <<slots'[self][m.o], self>>])})
                        ELSE /\ TRUE
                             /\ UNCHANGED << msgs, slots, log, own >>
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED << proposed, ballots >>

p2a(self) == /\ pc[self] = "p2a"
             /\ \E m \in msgs:
                  /\ m.type = "2a"
                  /\ m.b \succeq ballots[self][m.o]
                  /\ ballots' = [ballots EXCEPT ![self][m.o] = m.b]
                  /\ log' = [log EXCEPT ![self][m.o][m.s] = [b |-> m.b, v |-> m.v, commit |-> FALSE]]
                  /\ msgs' = (msgs \union {([type |-> "2b",
                                                n |-> self,
                                                o |-> m.o,
                                                b |-> m.b,
                                                s |-> m.s])})
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED << proposed, slots, own >>

p2b(self) == /\ pc[self] = "p2b"
             /\ \E m \in msgs:
                  /\ m.type = "2b"
                  /\ m.b = ballots[self][m.o]
                  /\ IF Q2Satisfied(ballots[self][m.o])
                        THEN /\ log' = [log EXCEPT ![self][m.o][m.s].commit = TRUE]
                             /\ msgs' = (msgs \union {([type |-> "3",
                                                           n |-> self,
                                                           o |-> m.o,
                                                           b |-> ballots[self][m.o],
                                                           s |-> m.s])})
                        ELSE /\ TRUE
                             /\ UNCHANGED << msgs, log >>
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED << proposed, ballots, slots, own >>

p3(self) == /\ pc[self] = "p3"
            /\ \E m \in msgs:
                 /\ m.type = "3"
                 /\ log' = [log EXCEPT ![self][m.o][m.s].commit = TRUE]
            /\ pc' = [pc EXCEPT ![self] = "start"]
            /\ UNCHANGED << msgs, proposed, ballots, slots, own >>

node(self) == start(self) \/ propose(self) \/ p1a(self) \/ p1b(self)
                 \/ p2a(self) \/ p2b(self) \/ p3(self)

Next == (\E self \in Nodes: node(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION


-----------------------------------------------------------------------------

(***************************************************************************)
(* Adding a key-value mapping (kv[1] is the key, kv[2] the value) to a map *)
(***************************************************************************)
f ++ kv == [x \in DOMAIN f \union {kv[1]} |-> IF x = kv[1] THEN kv[2] ELSE f[x]]

TypeOK == msgs \subseteq Messages
          
\*Safety == ~ (\A i,j \in Nodes,
\*                  o \in Objects,
\*                  s \in Slots :
\*             /\ log[i][o][s] # <<>>
\*             /\ log[i][o][s].commit = TRUE
\*             /\ log[j][o][s].commit = TRUE
\*             /\ log[i][o][s].v # log[j][o][s].v)
             
Safety == \A i, j \in Nodes, o \in Objects, s \in Slots :
          /\ log[i][o][s] # <<>>
          /\ log[j][o][s] # <<>>
          /\ log[i][o][s].commit = TRUE
          /\ log[j][o][s].commit = TRUE
          => log[i][o][s].v = log[j][o][s].v
             
Inv == TypeOK /\ Safety

=============================================================================
