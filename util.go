package paxi

import (
	"encoding/gob"
	"net"
	"time"

	"github.com/ailidani/paxi/log"
)

// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// VMax of a vector
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

// Schedule repeatedly call function with intervals
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

// ConnectToMaster connects to master node and set global Config
func ConnectToMaster(addr string, client bool) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	msg := &Register{
		Client: client,
		Addr:   "",
	}
	enc.Encode(msg)
	err = dec.Decode(&Config)
	if err != nil {
		log.Fatal(err)
	}
}
