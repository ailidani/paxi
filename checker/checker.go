package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/ailidani/paxi"
)

var file = flag.String("log", "log.csv", "")

func main() {
	flag.Parse()

	h := paxi.NewHistory()

	err := h.ReadFile(*file)
	if err != nil {
		log.Fatal(err)
	}

	n := h.Linearizable()

	fmt.Println(n)
}
