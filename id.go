package paxi

import (
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/ailidani/paxi/log"
)

var id = flag.String("id", "", "ID in format of Zone.Node.")

// ID represents a generic identifier in format of Zone.Node
type ID string

// GetID gets the current id specified in flag variables
func GetID() ID {
	if !flag.Parsed() {
		log.Warningln("Using ID before parse flag")
	}
	return ID(*id)
}

func NewID(zone, node int) ID {
	if zone < 0 {
		zone = -zone
	}
	if node < 0 {
		node = -node
	}
	return ID(fmt.Sprintf("%d.%d", zone, node))
}

// Zone returns Zond ID component
func (i ID) Zone() int {
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		return 0
	}
	s := strings.Split(string(i), ".")[0]
	zone, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Zone %s to int\n", s)
	}
	return int(zone)
}

// Node returns Node ID component
func (i ID) Node() int {
	var s string
	if !strings.Contains(string(i), ".") {
		log.Warningf("id %s does not contain \".\"\n", i)
		s = string(i)
	} else {
		s = strings.Split(string(i), ".")[1]
	}
	node, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Node %s to int\n", s)
	}
	return int(node)
}
