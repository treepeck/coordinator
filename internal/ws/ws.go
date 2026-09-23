package ws

import (
	"github.com/gorilla/websocket"
	"github.com/treepeck/coordinator/internal/transport"
	"github.com/treepeck/justchess/pkg/auth"
	"github.com/treepeck/justchess/pkg/db"
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
	// clients maps [client] to the URL they are connected to.
	clients map[*client]string
	ipc     transport.Ipc
	// Inbout WebSocket messages.
	inbound chan message
	// Incomming connections.
	join chan joinReq
	// Disconnected clients.
	leave chan *client
	// Estimated amount of active TCP connections.
	// TODO: might want to get rid of it and use len(ws.Service.sockets) somehow.
	// The problem is that len(ws.Service.sockets) is not safe for concurrent use.
	// One idea is to pass atomic.Int32 (TCP conn counter) in ipc struct.
	tcpConnsCache int
}

// InitService creates a new service and runs it's internal goroutines.
func InitService(gameRepo db.SQLGameRepo, ipc transport.Ipc) Service {
	s := Service{
		clients: make(map[*client]string, maxClients),
		inbound: make(chan message),
		join:    make(chan joinReq),
		leave:   make(chan *client),
		ipc:     ipc,
		// Single TCP connection is opened during initialization.
		tcpConnsCache: 1,
	}
	go s.listen()
	go s.publish()
	go s.route()
	return s
}

func (s Service) RegisterRoutes(authService auth.Service, mux *http.ServeMux) {
	// TODO: might want to have different endpoints for queue, player, and game spectator.
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
		s.ipc.Write <- proto.InMessage{
			PlayerId: m.clientId,
			Payload:  m.Payload,
		}
	}
}

// route routes outbound messages to WebSocket [client]s.
func (s Service) route() {
	for {
		m := <-s.ipc.Read
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

	// TODO: discard the connection if TCP cannot be adjusted.
	s.adjustTCPConns()

	url := req.r.URL.String()
	conn, err := upgrader.Upgrade(req.rw, req.r, nil)
	if err != nil {
		log.Printf("error while trying to upgrade the connection: %v\n", err)
		return
	}

	c := initClient(req.id, conn, s.leave, s.inbound)
	s.clients[c] = url

	// Notify JustChess about player connection.
	s.ipc.Write <- proto.InMessage{
		PlayerId: req.id,
		Payload: proto.Join(url),
	}

	log.Printf("register client %s\n", c.id)
}

func (s Service) unregister(c *client) {
	url, ok := s.clients[c]
	if !ok {
		log.Printf("client %s is not registered but is being unregistered\n", c.id)
		return
	}

	delete(s.clients, c)

	s.adjustTCPConns()

	// Notify JustChess about player disconnection.
	s.ipc.Write <- proto.InMessage{
		PlayerId: c.id,
		Payload: proto.Leave(url),
	}

	log.Printf("unregister client %s\n", c.id)
}

// adjustTCPConns adjusts the amount of opened TCP connections to the amount
// of active clients.
func (s Service) adjustTCPConns() {
	coeff := len(s.clients) / proto.ClientsPerConn
	if coeff == 0 || coeff == s.tcpConnsCache {
		return
	}

	res := make(chan int, 1)

	if coeff > s.tcpConnsCache {
		// Close unnecessary connection.
		s.ipc.Close <- res
	} else {
		// Open connection.
		s.ipc.Open <- res
	}

	conns := <-res
	if conns != -1 {
		s.tcpConnsCache = conns
	}
}
