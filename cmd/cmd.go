package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ailidani/paxi/chain"

	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/paxos"
)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
var master = flag.String("master", "", "Master address.")

func usage() string {
	s := "Usage:\n"
	s += "\t get key\n"
	s += "\t put key value\n"
	s += "\t consensus key\n"
	s += "\t crash id time\n"
	s += "\t partition time ids...\n"
	s += "\t exit\n"
	return s
}

var client paxi.Client
var admin paxi.AdminClient

func run(cmd string, args []string) {
	switch cmd {
	case "get":
		if len(args) < 1 {
			fmt.Println("get KEY")
			return
		}
		k, _ := strconv.Atoi(args[0])
		v, _ := client.Get(paxi.Key(k))
		fmt.Println(string(v))

	case "put":
		if len(args) < 2 {
			fmt.Println("put KEY VALUE")
			return
		}
		k, _ := strconv.Atoi(args[0])
		client.Put(paxi.Key(k), []byte(args[1]))
		//fmt.Println(string(v))

	case "consensus":
		if len(args) < 1 {
			fmt.Println("consensus KEY")
			return
		}
		k, _ := strconv.Atoi(args[0])
		v := admin.Consensus(paxi.Key(k))
		fmt.Println(v)

	case "crash":
		if len(args) < 2 {
			fmt.Println("crash id time(s)")
			return
		}
		id := paxi.ID(args[0])
		time, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("second argument should be integer")
			return
		}
		admin.Crash(id, time)

	case "partition":
		if len(args) < 2 {
			fmt.Println("partition time ids...")
			return
		}
		time, err := strconv.Atoi(args[0])
		if err != nil {
			fmt.Println("time argument should be integer")
			return
		}
		ids := make([]paxi.ID, 0)
		for _, s := range args[1:] {
			ids = append(ids, paxi.ID(s))
		}
		admin.Partition(time, ids...)

	case "exit":
		os.Exit(0)

	case "help":
		fallthrough
	default:
		fmt.Println(usage())
	}
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	admin = paxi.NewHTTPClient(paxi.ID(*id))

	switch *algorithm {
	case "paxos":
		client = paxos.NewClient(paxi.ID(*id))

	case "chain":
		client = chain.NewClient()

	default:
		client = paxi.NewHTTPClient(paxi.ID(*id))
	}

	if len(flag.Args()) > 0 {
		run(flag.Args()[0], flag.Args()[1:])
	} else {
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("paxi $ ")
			text, _ := reader.ReadString('\n')
			words := strings.Fields(text)
			if len(words) < 1 {
				continue
			}
			cmd := words[0]
			args := words[1:]

			run(cmd, args)
		}
	}
}
