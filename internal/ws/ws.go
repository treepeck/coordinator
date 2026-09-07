package ws

import (
	"github.com/gorilla/websocket"
	"github.com/treepeck/justchess/pkg/auth"
	"log"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "http://localhost:3502" },
}

const maxClients = 1000

// inReq represents the initial request sent by client to open a connection.
type inReq struct {
	id   string
	rw   http.ResponseWriter
	r    *http.Request
	wait chan struct{}
}

// Service is a HTTP server that serves a single specific endpoint called "/handshake".
// Handshake is a special HTTP request that the client platform sends to establish
// the WebSocket connection. Such request must have a specific format, defined
// in RFC 6455. Request validation along with protocol switching are handled
// entirely by the "gorilla/websocket" package. After it does it's job, the [Service]
// stores the connection object. All subsequent interaction between the client and
// [Service] occurs outside the scope of the handshake handler.
type Service struct {
	// TODO: this signature should be rewritten. The server must map room id to client's list.
	clients map[*client]struct{}
	// Incomming connections.
	in chan inReq
	// Disconnected clients.
	out chan *client
}

// InitService creates a new service and runs it's internal goroutines.
func InitService() Service {
	s := Service{
		clients: make(map[*client]struct{}, maxClients),
		in:      make(chan inReq),
		out:     make(chan *client),
	}
	go s.listen()
	return s
}

func (s Service) RegisterRoutes(authService auth.Service, mux *http.ServeMux) {
	mux.HandleFunc("GET /handshake", authService.MustAuthorize(s.handshake))
}

func (s Service) handshake(rw http.ResponseWriter, r *http.Request) {
	session, ok := r.Context().Value(auth.SessionKey).(auth.Session)
	if !ok {
		panic("request context is broken")
	}
	req := inReq{
		id:   session.Id,
		rw:   rw,
		r:    r,
		wait: make(chan struct{}),
	}
	s.in <- req
	<-req.wait
}

func (s Service) listen() {
	for {
		select {
		case req := <-s.in:
			s.register(req)
		case c := <-s.out:
			s.unregister(c)
		}
	}
}

func (s Service) register(req inReq) {
	defer func() {
		req.wait <- struct{}{}
	}()

	// Don't bother to upgrade the connection if the client limit is exceeded.
	if len(s.clients) == maxClients {
		req.rw.WriteHeader(http.StatusConflict)
		return
	}

	conn, err := upgrader.Upgrade(req.rw, req.r, nil)
	if err != nil {
		log.Printf("error while trying to upgrade the connection: %v\n", err)
		return
	}

	c := initClient(req.id, conn, s.out)
	s.clients[c] = struct{}{}
}

func (s Service) unregister(c *client) {
	delete(s.clients, c)
}
