package transport

import (
	"encoding/gob"
	"errors"
	"github.com/treepeck/justchess/pkg/proto"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
)

// Service manages the dynamic pool of TCP connections with the JustChess server.
type Service struct {
	sync.Mutex
	addr net.IP
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

	// TODO: maybe extract it to some other place.
	gob.Register(proto.Ping(0))
	gob.Register(proto.Pong(0))

	return Service{
		addr:    addr,
		port:    port,
		sockets: make(map[*socket]struct{}, proto.MaxConns),
	}, nil
}

func (s Service) OpenSocket() error {
	s.Lock()
	defer s.Unlock()

	if len(s.sockets) == proto.MaxConns {
		return errors.New("connection limit reached")
	}

	c, err := net.DialTCP("tcp", nil, &net.TCPAddr{
		IP:   s.addr,
		Port: s.port,
	})
	if err != nil {
		return err
	}

	sock := initSocket(c)
	s.sockets[sock] = struct{}{}
	log.Printf("open %v socket\n", sock)
	return nil
}

// CloseSocket closes a socket with a highest latency value.
func (s Service) CloseSocket() error {
	s.Lock()
	defer s.Unlock()

	var slowest *socket
	for sock := range s.sockets {
		if slowest == nil || sock.latency.Load() > slowest.latency.Load() {
			slowest = sock
		}
	}
	if slowest == nil {
		return errors.New("no opened sockets")
	}
	delete(s.sockets, slowest)
	log.Printf("close %v socket\n", slowest)
	return slowest.conn.Close()
}
