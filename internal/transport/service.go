package transport

import (
	"encoding/gob"
	"errors"
	"github.com/treepeck/justchess/pkg/proto"
	"log"
	"net"
	"os"
	"strconv"
)

// Service manages the dynamic pool of TCP connections with the JustChess server.
type Service struct {
	addr  net.IP
	Write chan proto.InMessage
	Read  chan proto.OutMessage
	Open  chan chan error
	Close chan chan error
	Poke  chan chan int
	port  int
	// Set of active [socket]s.
	sockets map[*socket]struct{}
}

func InitService() (Service, error) {
	addr := net.ParseIP(os.Getenv("JUSTCHESS_TCP_ADDR"))
	if addr == nil {
		return Service{}, errors.New("justchess: JUSTCHESS_TCP_ADDR environment variable must be not nil")
	}

	port, err := strconv.Atoi(os.Getenv("JUSTCHESS_TCP_PORT"))
	if err != nil {
		return Service{}, err
	}

	// TODO: maybe extract it to some other place.
	gob.Register(proto.Ping(0))
	gob.Register(proto.Pong(0))

	s := Service{
		addr:    addr,
		port:    port,
		Open:    make(chan chan error),
		Close:   make(chan chan error),
		Poke:    make(chan chan int),
		Write:   make(chan proto.InMessage, 256),
		Read:    make(chan proto.OutMessage, 256),
		sockets: make(map[*socket]struct{}, proto.MaxConns),
	}

	go s.listen()

	return s, nil
}

func (s Service) listen() {
	for {
		select {
		case res := <-s.Open:
			s.openSocket(res)
		case res := <-s.Close:
			s.closeSocket(res)
		case m := <-s.Write:
			s.writeSocket(m)
		}
	}
}

func (s Service) openSocket(res chan<- error) {
	if len(s.sockets) == proto.MaxConns {
		res <- errors.New("transport: connection limit reached")
	}

	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   s.addr,
		Port: s.port,
	})
	if err != nil {
		res <- err
		return
	}

	sock := initSocket(conn)
	s.sockets[sock] = struct{}{}
	log.Printf("open %v socket\n", sock)
}

// closeSocket closes a socket with a highest latency value.
func (s Service) closeSocket(res chan<- error) {
	var slowest *socket
	for sock := range s.sockets {
		if slowest == nil || sock.latency.Load() > slowest.latency.Load() {
			slowest = sock
		}
	}
	if slowest == nil {
		res <- errors.New("transport: no opened sockets")
	}
	delete(s.sockets, slowest)
	log.Printf("close %v socket\n", slowest)
	res <- slowest.conn.Close()
}

func (s Service) writeSocket(m proto.InMessage) {
	// TODO: proper load balancing between sockets.
	// Right now simply find socket with the lowest latency and write message to it.
	var fastest *socket
	for sock := range s.sockets {
		if fastest == nil || sock.latency.Load() < fastest.latency.Load() {
			fastest = sock
		}
	}
	if fastest == nil {
		return
	}

	fastest.send <- m

	// If there are more than one message awaiting delivery, send them in batch.
	for range len(s.Write) {
		fastest.send <- <-s.Write
	}
}
