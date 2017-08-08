package paxi

import (
	"fmt"
	"strconv"
)

// ID represents a generic identifier which is canonically
// stored as a uint64 but is typically represented as a
// base-16 string for input/output
type ID uint16

func NewID(sid uint8, nid uint8) ID {
	return ID(uint16(sid)<<8 + uint16(nid))
}

func (i ID) String() string {
	//return strconv.FormatUint(uint64(i), 10)
	return fmt.Sprintf("ID[%d:%d]", i.Site(), i.Node())
}

func (i ID) Site() uint8 {
	return uint8(uint16(i) >> 8)
}

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
