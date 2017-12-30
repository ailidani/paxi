package paxi

// NextBallot generates next ballot number given current ballot bumber and node id
func NextBallot(ballot int, id ID) int {
	n := id.Zone()<<16 | id.Node()
	return (ballot>>32+1)<<32 | n
}

// LeaderID return the node id from ballot number
func LeaderID(ballot int) ID {
	zone := uint32(ballot) >> 16
	node := uint16(ballot)
	return NewID(int(zone), int(node))
}
