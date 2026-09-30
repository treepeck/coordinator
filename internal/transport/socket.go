package transport

import (
	"bufio"
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
	ipc        Ipc
	conn       *net.TCPConn
	reader     *bufio.Reader
	writer     *bufio.Writer
	pingTicker *time.Ticker
	// Network latency in milliseconds.
	latency  *atomic.Int64
	lastPing *atomic.Int64
}

func initSocket(ipc Ipc, conn *net.TCPConn) *socket {
	// Wrap connection with buffer to reduce the amount of syscalls.
	// TODO: adjust the buffer size for peformance.
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)

	s := &socket{
		ipc:        ipc,
		conn:       conn,
		reader:     r,
		writer:     w,
		pingTicker: time.NewTicker(pingInterval),
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

// read continuously reades messages from the connection.
func (s *socket) read() {
	parts := proto.PreallocateDecodeBuff()

	// TODO: might be a race condition. Need to test that.
	encoded := make([]byte, proto.MaxMessageLength)
	var m proto.Message
	var err error
	for {
		encoded, err = s.reader.ReadBytes(proto.MessageSeparator)
		if err != nil {
			log.Printf("read error: %v\n", err)
			break
		}
		// Immediately handle pong messages.
		if proto.MessageKind(encoded[0]) == proto.KindPong {
			// Calculate the updated latency.
			s.latency.Store(time.Now().UnixMilli() - s.lastPing.Load())
			log.Printf("socket %v has latency %d\n", s, s.latency.Load())
		} else {
			m = proto.Decode(parts, encoded, proto.MessageKind(encoded[0]))
			log.Printf("Got a message: %v\n", m)
		}
	}

	s.cleanup()
}

// write continuously writes messages to the connection. It periodically send
// Ping messages to maintain the connection health.
func (s *socket) write() {
	for {
		select {
		case encoded := <-s.ipc.Write:
			if _, err := s.writer.Write(encoded); err != nil {
				log.Printf("write error: %v\n", err)
				break
			}
		case <-s.pingTicker.C:
			pingBuff := [1 + 8 + 1]byte{} // Kind + latency + message sep.
			i := 0
			pingBuff[i] = byte(proto.KindPing)
			// Manually encode Ping messages to avoid allocating a whole encoder for
			// every socket. Split integer into the byte sequence and write it to the buffer.
			lat := s.latency.Load()
			for ; lat != 0; lat >>= 8 {
				i++
				pingBuff[i] = byte(lat & 0xFF)
			}
			pingBuff[i+1] = proto.MessageSeparator

			if _, err := s.writer.Write(pingBuff[:i+2]); err != nil {
				log.Printf("write error: %v\n", err)
				break
			}
			s.lastPing.Store(time.Now().UnixMilli())
		}
		s.writer.Flush()
	}
}

func (s *socket) cleanup() {
	s.conn.Close()
	s.pingTicker.Stop()
}
