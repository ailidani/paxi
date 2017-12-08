-------------------------------- MODULE wpaxos-------------------------------
EXTENDS Integers, Sequences, FiniteSets, TLC
-----------------------------------------------------------------------------

(***************************************************************************)
(* Z number of zones                                                       *)
(***************************************************************************)
CONSTANT Z

(***************************************************************************)
(* f number of node failures in each zone                                  *)
(***************************************************************************)
CONSTANT f
N == 2 * f + 1

(***************************************************************************)
(* F number of zone failures                                               *)
(***************************************************************************)
CONSTANT F

ASSUME ConstantAssumption == /\ Z \in Nat \ {0}
                             /\ f \in Nat \ {0}
                             /\ F \in Nat
                             /\ F >= 0
                             /\ F < Z

(***************************************************************************)
(* Total objects and values                                                *)
(***************************************************************************)
CONSTANTS Objects, MaxSlot

ASSUME MaxSlot > 0

(***************************************************************************)
(* Definitions                                                             *)
(***************************************************************************)
Zones == 1..Z
Nodes == Zones \X (1..N)
Leaders == Zones \X {1}

ASSUME IsFiniteSet(Nodes)

vertical == {q \in SUBSET Nodes : /\ \A i,j \in q : i[1] = j[1]
                                  /\ Cardinality(q) = f + 1}

Q1 == {q \in SUBSET Nodes : /\ Cardinality(q) = (f + 1) * (Z - F)
                            /\ Cardinality({i[1] : i \in q}) = Z - F
                            /\ Cardinality({z \in vertical : z \subseteq q}) = Z - F}

Q2 == {q \in SUBSET Nodes : /\ Cardinality(q) = (f + 1) * (F + 1)
                            /\ Cardinality({i[1] : i \in q}) = F + 1
                            /\ Cardinality({z \in vertical : z \subseteq q}) = F + 1}

(***************************************************************************)
(* Quorum assumption                                                       *)
(***************************************************************************)
ASSUME QuorumAssumption == /\ \A q \in Q1 : q # {} /\ q \subseteq Nodes
                           /\ \A q \in Q2 : q # {} /\ q \subseteq Nodes
                           /\ \A q1 \in Q1 : \A q2 \in Q2 : q1 \cap q2 # {}

(***************************************************************************)
(* Ballot and Slot definition                                              *)
(***************************************************************************)
Ballots == Nat \X Nodes
Slots == 1..MaxSlot

(***************************************************************************)
(* All possible protocol messages:                                         *)
(***************************************************************************)
Messages ==     [type:{"1a"}, n:Nodes, o:Objects, b:Ballots]
         \union [type:{"1b"}, n:Nodes, o:Objects, b:Ballots, s:Slots \cup {0}]
         \union [type:{"2a"}, n:Nodes, o:Objects, b:Ballots, s:Slots, v:Slots \X Nodes]
         \union [type:{"2b"}, n:Nodes, o:Objects, b:Ballots, s:Slots]
         \union [type:{"3"}, n:Nodes, o:Objects, b:Ballots, s:Slots, v:Slots \X Nodes]

(***************************************************************************
(* wpaxos pluscal *)
--algorithm wpaxos {

    variable msgs = {};

    define {
        Q1Satisfied(o, b) == \E q \in Q1 : \A n \in q : \E m \in msgs : 
                             /\ m.type = "1b"
                             /\ m.o = o
                             /\ m.b = b
                             /\ m.n = n 
                             
        Q2Satisfied(o, b, s) == \E q \in Q2 : \A n \in q : \E m \in msgs : 
                                /\ m.type = "2b" 
                                /\ m.o = o
                                /\ m.b = b 
                                /\ m.s = s
                                /\ m.n = n

        more(a, b) == \/ a[1] > b[1]
                      \/ a[1] = b[1] /\ a[2][1] > b[2][1]
                      \/ a[1] = b[1] /\ a[2][1] = b[2][1] /\ a[2][2] > b[2][2]

        a \succ b == more(a, b)

        a \succeq b == a \succ b \/ a = b
        
        max(a, b) == IF a > b THEN a ELSE b
    }

    (* Send one message *)
    macro Send(m) { msgs := msgs \union {m}; }

    (* Sends multiple messages *)
    macro Sends(m) { msgs := msgs \union m; }

    macro p1a() {
        with(o \in Objects) {
            when self \in Leaders;
            when o \notin own;
            when ~ \E m \in msgs : m.type = "1a" /\ m.o = o;
            ballots[o] := <<ballots[o][1] + 1, self>>;
            Send([type |-> "1a",
                     n |-> self,
                     o |-> o,
                     b |-> ballots[o]]);
        }
    }
    
    macro p1b() {
        with (m \in msgs) {
            when m.type = "1a";
            when m.b \succeq ballots[m.o];
            ballots[m.o] := m.b;
            if (m.o \in own) own := own \ {m.o};
            Send([type |-> "1b",
                     n |-> self,
                     o |-> m.o,
                     b |-> m.b,
                     s |-> slots[m.o]]);
        }
    }
    
    macro p2a() {
        with (m \in msgs) {
            when m.type = "1b";
            when m.b = <<ballots[m.o][1], self>>;
            when m.o \notin own;
            when slots[m.o] < MaxSlot;
            if (Q1Satisfied(m.o, m.b)) {
                own := own \union {m.o};
                slots[m.o] := slots[m.o] + 1;
                log[m.o][slots[m.o]] := [b |-> m.b, v |-> <<slots[m.o], self>>, commit |-> FALSE];
                Send([type |-> "2a",
                         n |-> self,
                         o |-> m.o,
                         b |-> m.b,
                         s |-> slots[m.o],
                         v |-> <<slots[m.o], self>>]);
            }
        }
    }
    
    macro p2b() {
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
    
    macro p3() {
        with (m \in msgs) {
            when m.type = "2b";
            when m.b = <<ballots[m.o][1], self>>;
            when log[m.o][m.s].commit # TRUE;
            if (Q2Satisfied(m.o, m.b, m.s)) {
                log[m.o][m.s].commit := TRUE;
                Send([type |-> "3",
                         n |-> self,
                         o |-> m.o,
                         b |-> m.b,
                         s |-> m.s,
                         v |-> log[m.o][m.s].v]);
            }
        }
    }

    process (node \in Nodes)
    variables \* highest ballot numbers
              ballots = [o \in Objects |-> <<0, self>>];
              \* highest slot numbers
              slots = [o \in Objects |-> 0];
              \* log is a sequence of records [b -> Ballots, v -> Valuess, commit -> {TRUE, FALSE}]
              log = [o \in Objects |-> [s \in Slots |-> [b |-> 0, v |-> <<>>, commit |-> FALSE]]];
              \* objects I own
              own = {};
    {
        start: while(TRUE) {
            either p1a: p1a();
            or p1b: p1b();
            or p2a: p2a();
            or p2b: p2b();
            or p3: p3();
        }
    }
}
***************************************************************************)
\* BEGIN TRANSLATION
VARIABLES msgs, pc

(* define statement *)
Q1Satisfied(o, b) == \E q \in Q1 : \A n \in q : \E m \in msgs :
                     /\ m.type = "1b"
                     /\ m.o = o
                     /\ m.b = b
                     /\ m.n = n

Q2Satisfied(o, b, s) == \E q \in Q2 : \A n \in q : \E m \in msgs :
                        /\ m.type = "2b"
                        /\ m.o = o
                        /\ m.b = b
                        /\ m.s = s
                        /\ m.n = n

more(a, b) == \/ a[1] > b[1]
              \/ a[1] = b[1] /\ a[2][1] > b[2][1]
              \/ a[1] = b[1] /\ a[2][1] = b[2][1] /\ a[2][2] > b[2][2]

a \succ b == more(a, b)

a \succeq b == a \succ b \/ a = b

max(a, b) == IF a > b THEN a ELSE b

VARIABLES ballots, slots, log, own

vars == << msgs, pc, ballots, slots, log, own >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ msgs = {}
        (* Process node *)
        /\ ballots = [self \in Nodes |-> [o \in Objects |-> <<0, self>>]]
        /\ slots = [self \in Nodes |-> [o \in Objects |-> 0]]
        /\ log = [self \in Nodes |-> [o \in Objects |-> [s \in Slots |-> [b |-> 0, v |-> <<>>, commit |-> FALSE]]]]
        /\ own = [self \in Nodes |-> {}]
        /\ pc = [self \in ProcSet |-> "start"]

start(self) == /\ pc[self] = "start"
               /\ \/ /\ pc' = [pc EXCEPT ![self] = "p1a"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p1b"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p2a"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p2b"]
                  \/ /\ pc' = [pc EXCEPT ![self] = "p3"]
               /\ UNCHANGED << msgs, ballots, slots, log, own >>

p1a(self) == /\ pc[self] = "p1a"
             /\ \E o \in Objects:
                  /\ self \in Leaders
                  /\ o \notin own[self]
                  /\ ~ \E m \in msgs : m.type = "1a" /\ m.o = o
                  /\ ballots' = [ballots EXCEPT ![self][o] = <<ballots[self][o][1] + 1, self>>]
                  /\ msgs' = (msgs \union {([type |-> "1a",
                                                n |-> self,
                                                o |-> o,
                                                b |-> ballots'[self][o]])})
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED << slots, log, own >>

p1b(self) == /\ pc[self] = "p1b"
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
             /\ UNCHANGED << slots, log >>

p2a(self) == /\ pc[self] = "p2a"
             /\ \E m \in msgs:
                  /\ m.type = "1b"
                  /\ m.b = <<ballots[self][m.o][1], self>>
                  /\ m.o \notin own[self]
                  /\ slots[self][m.o] < MaxSlot
                  /\ IF Q1Satisfied(m.o, m.b)
                        THEN /\ own' = [own EXCEPT ![self] = own[self] \union {m.o}]
                             /\ slots' = [slots EXCEPT ![self][m.o] = slots[self][m.o] + 1]
                             /\ log' = [log EXCEPT ![self][m.o][slots'[self][m.o]] = [b |-> m.b, v |-> <<slots'[self][m.o], self>>, commit |-> FALSE]]
                             /\ msgs' = (msgs \union {([type |-> "2a",
                                                           n |-> self,
                                                           o |-> m.o,
                                                           b |-> m.b,
                                                           s |-> slots'[self][m.o],
                                                           v |-> <<slots'[self][m.o], self>>])})
                        ELSE /\ TRUE
                             /\ UNCHANGED << msgs, slots, log, own >>
             /\ pc' = [pc EXCEPT ![self] = "start"]
             /\ UNCHANGED ballots

p2b(self) == /\ pc[self] = "p2b"
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
             /\ UNCHANGED << slots, own >>

p3(self) == /\ pc[self] = "p3"
            /\ \E m \in msgs:
                 /\ m.type = "2b"
                 /\ m.b = <<ballots[self][m.o][1], self>>
                 /\ log[self][m.o][m.s].commit # TRUE
                 /\ IF Q2Satisfied(m.o, m.b, m.s)
                       THEN /\ log' = [log EXCEPT ![self][m.o][m.s].commit = TRUE]
                            /\ msgs' = (msgs \union {([type |-> "3",
                                                          n |-> self,
                                                          o |-> m.o,
                                                          b |-> m.b,
                                                          s |-> m.s,
                                                          v |-> log'[self][m.o][m.s].v])})
                       ELSE /\ TRUE
                            /\ UNCHANGED << msgs, log >>
            /\ pc' = [pc EXCEPT ![self] = "start"]
            /\ UNCHANGED << ballots, slots, own >>

node(self) == start(self) \/ p1a(self) \/ p1b(self) \/ p2a(self)
                 \/ p2b(self) \/ p3(self)

Next == (\E self \in Nodes: node(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION


-----------------------------------------------------------------------------

(***************************************************************************)
(* Adding a key-value mapping (kv[1] is the key, kv[2] the value) to a map *)
(***************************************************************************)
\*f ++ kv == [x \in DOMAIN f \union {kv[1]} |-> IF x = kv[1] THEN kv[2] ELSE f[x]]

TypeOK == msgs \subseteq Messages

(*
Safety == ~ (\A i,j \in Nodes,
                  o \in Objects,
                  s \in Slots :
             /\ log[i][o][s] # <<>>
             /\ log[i][o][s].commit = TRUE
             /\ log[j][o][s].commit = TRUE
             /\ log[i][o][s].v # log[j][o][s].v)

Safety == \A i, j \in Nodes, o \in Objects, s \in Slots :
          /\ log[i][o][s] # <<>>
          /\ log[j][o][s] # <<>>
          /\ log[i][o][s].commit = TRUE
          /\ log[j][o][s].commit = TRUE
          => log[i][o][s].v = log[j][o][s].v
*)

Safety == \A i,j \in msgs : /\ i.type = "3"
                            /\ j.type = "3"
                            /\ i.o = j.o
                            /\ i.s = j.s
                            => i.v = j.v

Inv == TypeOK /\ Safety

=============================================================================
