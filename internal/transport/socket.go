package transport

import (
	"bufio"
	"encoding/gob"
	"github.com/treepeck/justchess/pkg/proto"
	"log"
	"net"
	"sync/atomic"
	"time"
)

const (
	pingInterval = 5 * time.Second
)

type socket struct {
	conn       *net.TCPConn
	encoder    *gob.Encoder
	decoder    *gob.Decoder
	reader     *bufio.Reader
	writer     *bufio.Writer
	pingTicker *time.Ticker
	// Network latency in milliseconds.
	latency  *atomic.Int64
	lastPing *atomic.Int64
}

func initSocket(conn *net.TCPConn) *socket {
	// Wrap connection with buffer to reduce the amount of syscalls.
	// TODO: adjust the buffer size for peformance.
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	s := &socket{
		conn:       conn,
		encoder:    gob.NewEncoder(w),
		decoder:    gob.NewDecoder(r),
		reader:     r,
		writer:     w,
		pingTicker: time.NewTicker(pingInterval),
		latency:    &atomic.Int64{},
		lastPing:   &atomic.Int64{},
	}

	// latency value must be not 0, since the GOB protocol does not send zero values.
	s.latency.Store(1)
	s.lastPing.Store(time.Now().UnixMilli())

	go s.listen()
	go s.heartbeat()

	return s
}

// listen is designed to run as a goroutine to continuously
// consume new messages from the connection.
func (s *socket) listen() {
	defer s.cleanup()

	for {
		var msg proto.Message
		if err := s.decoder.Decode(&msg); err != nil {
			log.Printf("decode error: %v\n", err)
			break
		}

		switch t := msg.Payload.(type) {
		case proto.Pong:
			lat := (time.Now().UnixMilli() - s.lastPing.Load()) / 2
			// WARN: Latency must be strictly above zero. Otherwise the heartbeat mechanist will break.
			if lat == 0 {
				lat++
			}
			log.Printf("latency: %d\n", lat)
			s.latency.Store(lat)

		default:
			log.Printf("message has invalid type %v\n", t)
		}
	}
}

// heartbeat is designed to run as a goroutine to periodically
// send Ping messages to game server to maintain the connection
// health.
func (s *socket) heartbeat() {
	for {
		<-s.pingTicker.C
		// TODO: close the connection if previous ping was not answered.
		msg := proto.Message{
			Payload: proto.Ping(s.latency.Load()),
		}
		if err := s.encoder.Encode(msg); err != nil {
			break
		}
		s.writer.Flush()
		log.Printf("ping")
		s.lastPing.Store(time.Now().UnixMilli())
	}
}

func (s *socket) cleanup() {
	s.conn.Close()
	s.pingTicker.Stop()
}
