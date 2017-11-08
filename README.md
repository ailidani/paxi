
## What is WPaxos?

**WPaxos** is a multileader Paxos protocol that provides low-latency and high-throughput consensus across wide-area network (WAN) deployments. Unlike statically partitioned multiple Paxos deployments, WPaxos perpetually adapts to the changing access locality through object stealing. Multiple concurrent leaders coinciding in different zones steal ownership of objects from each other using phase-1 of Paxos, and then use phase-2 to commit update-requests on these objects locally until they are stolen by other leaders. To achieve zone-local phase-2 commits, WPaxos adopts the flexible quorums idea in a novel manner, and appoints phase-2 acceptors to be at the same zone as their respective leaders.

WPaxos (WAN Paxos) paper can be found in https://arxiv.org/abs/1703.08905.


## What is Paxi?

**Paxi** is the framework that implements WPaxos and other Paxos protocol variants. Paxi provides most of the elements that any Paxos implementation or replication protocol needs, including network communication, state machine of a key-value store, client API and multiple types of quorum systems.


## What is included?

- [x] [WPaxos](https://arxiv.org/abs/1703.08905)
- [x] [EPaxos](https://dl.acm.org/citation.cfm?id=2517350)
- [x] KPaxos (Static partitioned Paxos)
- [ ] [Vertical Paxos](https://www.microsoft.com/en-us/research/wp-content/uploads/2009/08/Vertical-Paxos-and-Primary-Backup-Replication-.pdf)
- [ ] [WanKeeper](http://ieeexplore.ieee.org/abstract/document/7980095/)


# Build

1. Install [Go 1.9](https://golang.org/dl/).
2. [Download](https://github.com/wpaxos/paxi/archive/master.zip) WPaxos source code from GitHub page or use following command:
```
go get github.com/wpaxos/paxi
```

3. Move paxi directory to `$GOPATH/src/`
```
mv paxi $GOPATH/src/
```

4. Compile everything.
```
cd paxi/bin
./build.sh
```

After compile, Golang will generate 4 executable files under `bin` folder.
* `master` is the easy way to setup the cluster and distribute configurations to all replica nodes.
* `server` is one replica instance.
* `client` is a simple benchmark that generates read/write reqeust to servers.


# Run

Each executable file expects some parameters which can be seen by `-help` flag, e.g. `./master -help`.

1. Start master node with 6 replicas running WPaxos:
```
./master.sh -n 6 -protocol "wpaxos"
```

2. Start 6 servers with different zone id and node ids.
```
./server -sid 1 -nid 1 -master 127.0.0.1 &
./server -sid 1 -nid 2 -master 127.0.0.1 &
./server -sid 2 -nid 1 -master 127.0.0.1 &
./server -sid 2 -nid 2 -master 127.0.0.1 &
./server -sid 3 -nid 1 -master 127.0.0.1 &
./server -sid 3 -nid 2 -master 127.0.0.1 &
```

3. Start benchmarking client with 10 threads, 1000 keys, 50 percent conflicting commands and run for 60 seconds in 1 round.
```
./client -sid 1 -nid 1 -master 127.0.0.1 -T 10 -k 1000 -r 1 -c 50 -t 60 &
```


---
A new version of Paxi project under heavy development is also available in https://github.com/ailidani/paxi.
