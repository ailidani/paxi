[![GoDoc](https://godoc.org/github.com/ailidani/paxi?status.svg)](https://godoc.org/github.com/ailidani/paxi)
[![Go Report Card](https://goreportcard.com/badge/github.com/ailidani/paxi)](https://goreportcard.com/report/github.com/ailidani/paxi)
[![Build Status](https://travis-ci.org/ailidani/paxi.svg?branch=master)](https://travis-ci.org/ailidani/paxi)


## What is Paxi?

**Paxi** is the framework that implements WPaxos and other Paxos protocol variants. Paxi provides most of the elements that any Paxos implementation or replication protocol needs, including network communication, state machine of a key-value store, client API and multiple types of quorum systems.

*Warning*: Paxi project is still under heavy development, with more features and protocols to include. Paxi API may change too.


## What is WPaxos?

**WPaxos** is a multileader Paxos protocol that provides low-latency and high-throughput consensus across wide-area network (WAN) deployments. Unlike statically partitioned multiple Paxos deployments, WPaxos perpetually adapts to the changing access locality through object stealing. Multiple concurrent leaders coinciding in different zones steal ownership of objects from each other using phase-1 of Paxos, and then use phase-2 to commit update-requests on these objects locally until they are stolen by other leaders. To achieve fast phase-2 commits, WPaxos adopts the flexible quorums idea in a novel manner, and appoints phase-2 acceptors to be close to their respective leaders.

WPaxos (WAN Paxos) paper (first version) can be found in https://arxiv.org/abs/1703.08905.

## What is included?

Algorithms:
- [x] Classical multi-Paxos
- [x] [Flexible Paxos](https://dl.acm.org/citation.cfm?id=3139656)
- [x] [WPaxos](https://arxiv.org/abs/1703.08905)
- [x] [EPaxos](https://dl.acm.org/citation.cfm?id=2517350)
- [x] [SDPaxos](https://www.microsoft.com/en-us/research/uploads/prod/2018/09/172-zhao.pdf)
- [x] Atomic Storage ([Majority Replication](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.174.7245&rep=rep1&type=pdf))
- [x] [Chain Replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf)
- [x] KPaxos (Static partitioned Paxos)
- [x] [Dynamo Key-value Store](https://dl.acm.org/citation.cfm?id=1294281)
- [x] [WanKeeper](http://ieeexplore.ieee.org/abstract/document/7980095/)
- [x] [Vertical Paxos](https://www.microsoft.com/en-us/research/wp-content/uploads/2009/08/Vertical-Paxos-and-Primary-Backup-Replication-.pdf)


Features:
- [x] Benchmarking
- [x] Linerizability checker
- [x] Fault injection


# How to build

1. Install [Go](https://golang.org/dl/).
2. Use `go get` command or [Download](https://github.com/wpaxos/paxi/archive/master.zip) Paxi source code from GitHub page.
```
go get github.com/ailidani/paxi
```

3. Compile everything from `paxi/bin` folder.
```
cd github.com/ailidani/paxi/bin
./build.sh
```

After compile, Golang will generate 3 executable files under `bin` folder.
* `server` is one replica instance.
* `client` is a simple benchmark that generates read/write reqeust to servers.
* `cmd` is a command line tool to test Get/Set requests.


# How to run

Each executable file expects some parameters which can be seen by `-help` flag, e.g. `./server -help`.

1. Generate the [configuration file](https://github.com/ailidani/paxi/blob/master/bin/config.json) according to the example, then start server with `-config FILE_PATH` option, default to "config.json" when omit.

2. Start 9 servers with different ids in format of "ZONE_ID.NODE_ID".
```
./server -id 1.1 -algorithm=paxos &
./server -id 1.2 -algorithm=paxos &
./server -id 1.3 -algorithm=paxos &
./server -id 2.1 -algorithm=paxos &
./server -id 2.2 -algorithm=paxos &
./server -id 2.3 -algorithm=paxos &
./server -id 3.1 -algorithm=paxos &
./server -id 3.2 -algorithm=paxos &
./server -id 3.3 -algorithm=paxos &
```

3. Start benchmarking client that connects to server ID 1.1 and benchmark parameters specified in [config.json](https://github.com/ailidani/paxi/blob/master/bin/config.json).
```
./client -id 1.1 -bconfig benchmark.json
```
When flag `id` is absent, client will randomly select any server for each operation.

The algorithms can also be running in **simulation** mode, where all nodes are running in one process and transport layer is replaced by Go channels. Check [`simulation.sh`](https://github.com/ailidani/paxi/blob/master/bin/simulation.sh) script on how to run.


# How to implement algorithms in Paxi

Replication algorithm in Paxi follows the message passing model, where several message types and their handle function are registered. We use [Paxos](https://github.com/ailidani/paxi/tree/master/paxos) as an example for our step-by-step tutorial.

1. Define messages, register with gob in `init()` function if using gob codec. As show in [`msg.go`](https://github.com/ailidani/paxi/blob/master/paxos/msg.go).

2. Define a `Replica` structure embeded with `paxi.Node` interface.
```go
type Replica struct {
	paxi.Node
	*Paxos
}
```

Define handle function for each message type. For example, to handle client `Request`
```go
func (r *Replica) handleRequest(m paxi.Request) {
	if *adaptive {
		if r.Paxos.IsLeader() || r.Paxos.Ballot() == 0 {
			r.Paxos.HandleRequest(m)
		} else {
			go r.Forward(r.Paxos.Leader(), m)
		}
	} else {
		r.Paxos.HandleRequest(m)
	}

}
```

3. Register the messages with their handle function using `Node.Register(interface{}, func())` interface in `Replica` constructor.

Replica use `Send(to ID, msg interface{})`, `Broadcast(msg interface{})` functions in Node.Socket to send messages.

For data-store related functions check `db.go` file.

For quorum types check `quorum.go` file.

Client uses a simple RESTful API to submit requests. GET method with URL "http://ip:port/key" will read the value of given key. POST method with URL "http://ip:port/key" and body as the value, will write the value to key.
