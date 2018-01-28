package paxi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"path"
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
	k, _ := strconv.Atoi(path.Base(r.URL.Path))
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err := w.Write(b)
	if err != nil {
		log.Error(err)
	}
}
