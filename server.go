package paxi

import (
	"net/http"
	"net/rpc"
)

type Server struct {
	http string
}

func (s *Server) Get(request *Request, reply *Reply) error {
	return nil
}

func (s *Server) PUT(request *Request, reply *Reply) error {
	return nil
}

func (s *Server) start() {
	rpc.Register(s)
	rpc.HandleHTTP()
	http.ListenAndServe(s.http, nil)
}
