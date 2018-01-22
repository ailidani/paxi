package paxi

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"

	"github.com/ailidani/paxi/log"
)

func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	req.c = make(chan Reply)
	req.ClientID = ID(r.Header.Get("id"))
	cid, _ := strconv.ParseUint(r.Header.Get("cid"), 10, 64)
	req.CommandID = CommandID(cid)
	req.Timestamp, _ = strconv.ParseInt(r.Header.Get("timestamp"), 10, 64)

	if len(r.URL.Path) > 1 {
		i, _ := strconv.Atoi(r.URL.Path[1:])
		key := Key(i)
		switch r.Method {
		case http.MethodGet:
			req.Command = Command{GET, key, nil}
		case http.MethodPut, http.MethodPost:
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Errorln("error reading body: ", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			req.Command = Command{PUT, key, Value(body)}
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorln("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		json.Unmarshal(body, &req)
	}

	n.MessageChan <- req

	reply := <-req.c
	if reply.Err != nil {
		http.Error(w, reply.Err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("id", fmt.Sprintf("%v", reply.ClientID))
	w.Header().Set("cid", fmt.Sprintf("%v", reply.CommandID))
	w.Header().Set("timestamp", fmt.Sprintf("%v", reply.Timestamp))
	_, err := io.WriteString(w, string(reply.Command.Value))
	// _, err := r.w.Write(reply.Command.Value)
	if err != nil {
		log.Errorln(err)
	}
}

func (n *node) handleHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("id", string(n.id))
	k, _ := strconv.Atoi(path.Base(r.URL.Path))
	h := n.Database.History(Key(k))
	b, _ := json.Marshal(h)
	_, err := w.Write(b)
	if err != nil {
		log.Error(err)
	}
}
