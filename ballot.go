package paxi

// NextBallot generates next ballot number given current ballot bumber and node id
func NextBallot(ballot int, id ID) int {
	return (ballot>>16+1)<<16 | int(id)
}

// LeaderID return the node id from ballot number
func LeaderID(ballot int) ID {
	return ID(uint16(ballot))
}
