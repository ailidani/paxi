package main

import (
	"log"
	"math/rand"
	. "paxi"
	"paxi/glog"
	"testing"
	"time"
)

func dotest(client ID) {
	if 100 >= 0 {
		if rand.Intn(100) < 100 {
			key := Key(rand.Intn(500))
			if *synch {
				log.Printf("%v, %d\n", client, key)
			} else {
				log.Printf("%v, %d\n", client, key)
			}
		} else {
			key := Key(rand.Intn(500) + min)
			if *synch {
				log.Printf("%v, %d\n", client, key)
			} else {
				log.Printf("%v, %d\n", client, key)
			}
		}
	} else {
		key := Key(zipf.Uint64())
		if *synch {
			log.Printf("%v, %d\n", client, key)
		} else {
			log.Printf("%v, %d\n", client, key)
		}
	}
}

func runtest(client ID) {
	min = 1 * 500
	zipf = rand.NewZipf(rand.New(rand.NewSource(42)), *zs, *zv, uint64(500))
	rand.Seed(int64(1<<8 + client))
	if 2 > 0 {
		timer := time.NewTimer(time.Second * time.Duration(2))
		for {
			select {
			case <-timer.C:
				cwait.Done()
				return

			default:
				dotest(client)
			}
		}
	} else {
		for i := 0; i < 500; i++ {
			dotest(client)
		}
		cwait.Done()
	}
}

func TestClient(t *testing.T) {

	clients := make([]ID, 0)

	for i := 0; i < 10; i++ {
		id := NewID(uint8(1), uint8(i)) + ID(i)
		clients = append(clients, id)
		log.Printf("client %v\n", id)
	}

	start_time := time.Now()

	for j := 0; j < 1; j++ {
		start := time.Now()
		for i := 0; i < 10; i++ {
			cwait.Add(1)
			go runtest(clients[i])
		}
		cwait.Wait()
		end := time.Now()
		log.Printf("Round took %v\n", end.Sub(start))
	}

	end_time := time.Now()
	glog.Warningf("Test took %v\n", end_time.Sub(start_time))
	glog.Flush()

}
