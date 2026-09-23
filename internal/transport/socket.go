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

// TODO: Reconnect.
type socket struct {
	conn       *net.TCPConn
	encoder    *gob.Encoder
	decoder    *gob.Decoder
	reader     *bufio.Reader
	writer     *bufio.Writer
	pingTicker *time.Ticker
	send       chan proto.InMessage
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
		send:       make(chan proto.InMessage, 256),
		latency:    &atomic.Int64{},
		lastPing:   &atomic.Int64{},
	}

	// latency value must be not 0, since the GOB protocol does not send zero values.
	s.latency.Store(1)
	s.lastPing.Store(time.Now().UnixMilli())

	go s.read()
	go s.write()

	return s
}

// read is designed to run as a goroutine to continuously
// consume messages from the connection.
func (s *socket) read() {
	defer s.cleanup()

	for {
		var msg proto.OutMessage
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

// write is designed to run as a goroutine to continuously write messages
// to the connection. Additionaly, it periodically send Ping messages to
// maintain the connection health.
func (s *socket) write() {
	for {
		var msg proto.InMessage

		select {
		case <-s.pingTicker.C:
			// TODO: close the connection if previous ping was not answered.
			msg.Payload = proto.Ping(s.latency.Load())
			log.Printf("ping")
			s.lastPing.Store(time.Now().UnixMilli())

		case m := <-s.send:
			msg = m
		}

		if err := s.encoder.Encode(msg); err != nil {
			log.Printf("cannot encode message: %v\n", err)
			break
		}
		s.writer.Flush()
	}
}

func (s *socket) cleanup() {
	s.conn.Close()
	s.pingTicker.Stop()
}
