package transport

import (
	"errors"
	"github.com/treepeck/justchess/pkg/proto"
	"log"
	"net"
	"os"
	"strconv"
)

// Ipc wraps all channels used for inter-process communication between [transport]
// and [ws] packages.
type Ipc struct {
	// Open is used to add a new TCP connection. The [chan int] returns the number
	//  of active TCP connections. Special case -1, which indicates an error.
	Open chan chan int
	// Close is used to remove an opened TCP connection. Connection with the biggest
	// latency will be removed. The [chan int] returns the number of active TCP
	// connections. Special case -1, which indicates an error.
	// TODO: might want to delete the least used connection.
	Close chan chan int
	// Write is used to write messages to TCP connection pool.
	Write chan proto.InMessage
	// Read is used to read messages from TCP connection pool.
	Read chan proto.OutMessage
}

// Service manages the dynamic pool of TCP connections with the JustChess server.
type Service struct {
	addr net.IP
	Ipc  Ipc
	port int
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

	proto.RegisterGOBTypes()

	s := Service{
		Ipc: Ipc{
			Open:  make(chan chan int),
			Close: make(chan chan int),
			Write: make(chan proto.InMessage, 256),
			Read:  make(chan proto.OutMessage, 256),
		},
		addr:    addr,
		port:    port,
		sockets: make(map[*socket]struct{}, proto.MaxConns),
	}

	go s.listen()

	return s, nil
}

func (s Service) listen() {
	// TODO: defer s.cleanup() to close all conns.

	for {
		select {
		case res := <-s.Ipc.Open:
			s.openSocket(res)
		case res := <-s.Ipc.Close:
			s.closeSocket(res)
		}
	}
}

func (s Service) openSocket(res chan<- int) {
	if len(s.sockets) == proto.MaxConns {
		log.Print("TCP connection limit reached")
		res <- -1
		return
	}

	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   s.addr,
		Port: s.port,
	})
	if err != nil {
		log.Printf("cannot open TCP connection: %v\n", err)
		res <- -1
		return
	}

	sock := initSocket(s.Ipc, conn)
	s.sockets[sock] = struct{}{}
	log.Printf("opened new TCP socket %v\n", sock)

	res <- len(s.sockets)
}

// closeSocket closes a socket with a highest latency value.
func (s Service) closeSocket(res chan<- int) {
	var slowest *socket
	for sock := range s.sockets {
		if slowest == nil || sock.latency.Load() > slowest.latency.Load() {
			slowest = sock
		}
	}
	if slowest == nil {
		log.Print("no active TCP sockets")
		res <- -1
		return
	}
	delete(s.sockets, slowest)
	slowest.conn.Close() // TODO: might want to handle error.
	log.Printf("closed TCP socket %v\n", slowest)

	res <- len(s.sockets)
}
