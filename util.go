package paxi

import (
	"encoding/gob"
	"log"
	"net"
	"strconv"
	"time"
)

func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

func NextBallot(ballot int, id ID) int {
	return (ballot>>16+1)<<16 | int(id)
}

// func NextBallot(ballot int, id ID) int {
// 	return ballot
// }

func LeaderID(ballot int) ID {
	return ID(uint16(ballot))
}

func Schedule(what func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			what()
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}

func ConnectToMaster(addr string, t EndpointType, id ID) *Config {
	gob.Register(Register{})
	gob.Register(Config{})
	conn, err := net.Dial("tcp", addr+":"+strconv.Itoa(PORT))
	if err != nil {
		log.Fatalln(err)
	}
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	msg := &Register{
		EndpointType: t,
		ID:           id,
		Addr:         "",
	}
	enc.Encode(msg)
	var config Config
	err = dec.Decode(&config)
	if err != nil {
		log.Fatalln(err)
	}
	return &config
}
