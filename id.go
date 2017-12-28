package paxi

import (
	"flag"
	"strconv"

	"github.com/ailidani/paxi/log"
)

var zid = flag.Int("zid", 1, "Zone ID.")
var nid = flag.Int("nid", 1, "Node ID.")

// ID represents a generic identifier which is canonically
// stored as a uint16 but is typically represented as a
// base-16 string for input/output
type ID uint16

// GetID gets the current id specified in flag variables
func GetID() ID {
	if !flag.Parsed() {
		log.Warningln("Using ID before parse flag")
	}
	return ID(uint16(*zid)<<8 + uint16(*nid))
}

// NewID generates a new ID based on given variable
func NewID(zid, nid uint8) ID {
	return ID(uint16(zid)<<8 + uint16(nid))
}

func (i ID) String() string {
	return strconv.FormatUint(uint64(i), 10)
	// return fmt.Sprintf("ID[%d:%d]", i.Zone(), i.Node())
}

// Zone returns Zond ID component
func (i ID) Zone() uint8 {
	return uint8(uint16(i) >> 8)
}

// Node returns Node ID component
func (i ID) Node() uint8 {
	return uint8(uint16(i))
}

// IDFromString attempts to create an ID from a base-16 string.
func IDFromString(s string) (ID, error) {
	i, err := strconv.ParseUint(s, 10, 64)
	return ID(i), err
}

// IDSlice implements the sort interface
type IDSlice []ID

func (p IDSlice) Len() int           { return len(p) }
func (p IDSlice) Less(i, j int) bool { return uint64(p[i]) < uint64(p[j]) }
func (p IDSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
