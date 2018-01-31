package paxi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ailidani/paxi/log"
)

const (
	// http request header names
	HttpClientID  = "id"
	HttpCommandID = "cid"
	HttpTimestamp = "timestamp"
	HttpNodeID    = "id"
)

// serve serves the http REST API request from clients
func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
	mux.HandleFunc("/history", n.handleHistory)
	mux.HandleFunc("/crash", n.handleCrash)
	mux.HandleFunc("/drop", n.handleDrop)
	// http string should be in form of ":8080"
	url, err := url.Parse(n.config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	log.Fatal(http.ListenAndServe(port, mux))
}

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var cmd Command
	cmd.ClientID = ID(r.Header.Get(HttpClientID))
	cmd.CommandID, _ = strconv.Atoi(r.Header.Get(HttpCommandID))
	timestamp, _ := strconv.ParseInt(r.Header.Get(HttpTimestamp), 10, 64)
	if len(r.URL.Path) > 1 {
		i, err := strconv.Atoi(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i)
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorln("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			cmd.Value = Value(body)
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorln("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		json.Unmarshal(body, &cmd)
	}

	req := Request{
		Command:   cmd,
		Timestamp: timestamp,
		c:         make(chan Reply),
	}

	n.MessageChan <- req

	reply := <-req.c

	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set(HttpClientID, string(reply.Command.ClientID))
	w.Header().Set(HttpCommandID, strconv.Itoa(reply.Command.CommandID))
	w.Header().Set(HttpTimestamp, strconv.FormatInt(reply.Timestamp, 10))
	_, err := io.WriteString(w, string(reply.Value))
	if err != nil {
		log.Errorln(err)
	}
}

func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(HttpNodeID, string(n.id))
	k, err := strconv.Atoi(r.URL.Query().Get("key"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide key", http.StatusBadRequest)
		return
	}
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err)
	}
}

func (n *node) handleCrash(w http.ResponseWriter, r *http.Request) {
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Crash(t)
}

func (n *node) handleDrop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	t, err := strconv.Atoi(r.URL.Query().Get("t"))
	if err != nil {
		log.Error(err)
		http.Error(w, "invalide time", http.StatusBadRequest)
		return
	}
	n.Drop(ID(id), t)
}
