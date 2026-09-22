package ws

import (
	"github.com/gorilla/websocket"
	"github.com/treepeck/justchess/pkg/auth"
	"github.com/treepeck/justchess/pkg/proto"
	"log"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return r.Header.Get("Origin") == "http://localhost:3502" },
}

const maxClients = 1000

// joinReq represents the initial request sent by client to open a connection.
type joinReq struct {
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
	// TODO: store which clients are subscribed to which topics.
	clients map[*client]struct{}
	// Inbout WebSocket messages.
	inbound chan message
	// open is used to add a new TCP connection.
	open chan chan error
	// close is used to remove an opened TCP connection.
	close chan chan error
	// poke is used to get the number of opened TCP connections.
	poke chan chan int
	// write is used to write messages to TCP connection pool.
	write chan proto.InMessage
	// read is used to read messages from TCP connection pool.
	read chan proto.OutMessage
	// Incomming connections.
	join chan joinReq
	// Disconnected clients.
	leave chan *client
}

// InitService creates a new service and runs it's internal goroutines.
func InitService(open, close chan chan error, poke chan chan int, write chan proto.InMessage, read chan proto.OutMessage) Service {
	s := Service{
		clients: make(map[*client]struct{}, maxClients),
		inbound: make(chan message),
		join:    make(chan joinReq),
		leave:   make(chan *client),
		open:    open,
		close:   close,
		poke: 	poke,
		write:   write,
		read:    read,
	}
	go s.listen()
	go s.publish()
	go s.route()
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
	req := joinReq{
		id:   session.Id,
		rw:   rw,
		r:    r,
		wait: make(chan struct{}),
	}
	s.join <- req
	<-req.wait
}

// listen listens for join and leave requests.
func (s Service) listen() {
	for {
		select {
		case req := <-s.join:
			s.register(req)
		case c := <-s.leave:
			s.unregister(c)
		}
	}
}

// publish publishes inbound WebSocket messages to transport package.
func (s Service) publish() {
	for {
		m := <-s.inbound
		s.write <- proto.InMessage{
			PlayerId: m.clientId,
			Payload:  m.Payload,
		}
	}
}

// route routes outbound messages to WebSocket [client]s.
func (s Service) route() {
	for {
		m := <-s.read
		log.Printf("recieved message from TCP conn: %v\n", m)
	}
}

func (s Service) register(req joinReq) {
	defer func() {
		req.wait <- struct{}{}
	}()

	// Don't bother to upgrade the connection if the client limit is exceeded.
	if len(s.clients) == maxClients {
		req.rw.WriteHeader(http.StatusConflict)
		return
	}

	// Open new connection whenever [proto.ClientsPerConn] limit is exceeded.
	tcpConns := make(chan int)
	s.poke <- tcpConns
	if len(s.clients) / proto.ClientsPerConn > <-tcpConns {
		res := make(chan error)
		s.open <- res
		if err := <-res; err != nil {
			log.Print(err)
			return
		}
	}

	conn, err := upgrader.Upgrade(req.rw, req.r, nil)
	if err != nil {
		log.Printf("error while trying to upgrade the connection: %v\n", err)
		return
	}

	c := initClient(req.id, conn, s.leave, s.inbound)
	s.clients[c] = struct{}{}
}

func (s Service) unregister(c *client) {
	delete(s.clients, c)

	// Close unnecessary connections to free resourses.
}
