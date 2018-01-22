package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
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
			log.Println(v)

		case "put":
			if len(args) < 2 {
				fmt.Println("put KEY VALUE")
				continue
			}
			k, _ := strconv.Atoi(args[0])
			v := client.Put(paxi.Key(k), []byte(args[1]))
			log.Println(v)

		case "consensus":
			if len(args) < 1 {
				log.Println("consensus KEY")
				continue
			}
			k, _ := strconv.Atoi(args[0])
			v := client.Consensus(paxi.Key(k))
			log.Println(v)

		case "exit":
			os.Exit(0)

		default:
			log.Println(usage())
		}

	}
}
