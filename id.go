package paxi

import (
	"strconv"
	"strings"

	"github.com/ailidani/paxi/log"
)

// ID represents a generic identifier in format of Zone.Node
type ID string

// NewID returns a new ID type given two int number of zone and node
func NewID(zone, node int) ID {
	if zone < 0 {
		zone = -zone
	}
	if node < 0 {
		node = -node
	}
	// return ID(fmt.Sprintf("%d.%d", zone, node))
	return ID(strconv.Itoa(zone) + "." + strconv.Itoa(node))
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
		return 0
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
		return 0
	}
	return int(node)
}

type IDs []ID

func (a IDs) Len() int      { return len(a) }
func (a IDs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a IDs) Less(i, j int) bool {
	if a[i].Zone() < a[j].Zone() {
		return true
	} else if a[i].Zone() > a[j].Zone() {
		return false
	} else {
		return a[i].Node() < a[j].Node()
	}
}
