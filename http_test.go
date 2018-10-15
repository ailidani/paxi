package paxi

import (
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
)

func RunServer(t *testing.T, port string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("id")
		i := r.URL.Path[1:]

		t.Log("server", id, i)

		w.Header().Set("id", id)
		_, err := io.WriteString(w, i)
		if err != nil {
			t.Fatal(err)
		}
	})
	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}
	go func() {
		err := server.ListenAndServe()
		if err != http.ErrServerClosed {
			t.Fatal(err)
		}
	}()
	return server
}

func RunClient(t *testing.T, port string) {
	i := rand.Intn(1000)
	url := "http://127.0.0.1:" + port + "/" + strconv.Itoa(i)
	r, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
		return
	}
	r.Header.Set("id", strconv.Itoa(i))
	res, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal("client", err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		b, _ := ioutil.ReadAll(res.Body)
		t.Log("client", string(b))
	}
}

func TestHTTP(t *testing.T) {
	s := RunServer(t, "8087")
	RunClient(t, "8087")
	err := s.Shutdown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}
