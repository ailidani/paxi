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

var master = flag.String("master", "", "Master address.")

func usage() string {
	return fmt.Sprintf("\n\t get key \n\t put key value \n\t consensus key \n\t exit")
}

func main() {
	flag.Parse()

	id := paxi.GetID()
	var config paxi.Config
	if *master == "" {
		config = paxi.NewConfig(id)
	} else {
		config = paxi.ConnectToMaster(*master, true, id)
	}

	client := paxi.NewClient(config)
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

		default:
			fmt.Println(usage())
		}

	}
}
