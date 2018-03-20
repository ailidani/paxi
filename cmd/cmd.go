package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ailidani/paxi"
)

var id = flag.String("id", "", "node id this client connects to")
var master = flag.String("master", "", "Master address.")

func usage() string {
	return fmt.Sprint("\t get key \n\t put key value \n\t consensus key \n\t crash id \n\t exit")
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	client := paxi.NewClient(paxi.ID(*id))
	client.Start()

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

		switch cmd {
		case "get":
			if len(args) < 1 {
				fmt.Println("get KEY")
				continue
			}
			k, _ := strconv.Atoi(args[0])
			v := client.Get(paxi.Key(k))
			fmt.Println(string(v))

		case "put":
			if len(args) < 2 {
				fmt.Println("put KEY VALUE")
				continue
			}
			k, _ := strconv.Atoi(args[0])
			v := client.Put(paxi.Key(k), []byte(args[1]))
			fmt.Println(string(v))

		case "consensus":
			if len(args) < 1 {
				fmt.Println("consensus KEY")
				continue
			}
			k, _ := strconv.Atoi(args[0])
			v := client.Consensus(paxi.Key(k))
			fmt.Println(v)

		case "crash":
			if len(args) < 2 {
				fmt.Println("crash id time(s)")
				continue
			}
			id := paxi.ID(args[0])
			time, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Println("second argument should be integer")
				continue
			}
			client.Crash(id, time)

		case "exit":
			os.Exit(0)

		case "help":
			fallthrough
		default:
			fmt.Println(usage())
		}

	}
}
