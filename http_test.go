package paxi

import (
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

func ServerRun(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("id")
		i := r.URL.Path[1:]

		log.Println("server", id, i)
		time.Sleep(time.Second)

		w.Header().Set("id", id)
		_, err := io.WriteString(w, i)
		if err != nil {
			log.Fatal(err)
		}
	})
	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	log.Fatal(server.ListenAndServe())
}

var wg sync.WaitGroup

func ClientRun(port string) {
	i := rand.Intn(1000)
	url := "http://127.0.0.1:" + port + "/" + strconv.Itoa(i)
	r, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	r.Header.Set("id", strconv.Itoa(i))
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		log.Fatal("client", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		log.Println("client", string(b))
	}
	wg.Done()
}

func TestHTTP(t *testing.T) {
	go ServerRun("8087")
	for i := 0; i < 100; i++ {
		for j := 0; j < 100; j++ {
			wg.Add(1)
			go ClientRun("8087")
		}
		wg.Wait()
	}
}
