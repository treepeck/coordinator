package ws

import (
	"encoding/json"
	"github.com/gorilla/websocket"
	"log"
	"time"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second
	// Maximum message size in bytes.
	maxMessageSize = 1024
)

// client is a wrapper around the connection object. It incapsulates the
// process of reading and writing messages for a single connection.
// It also stores the network delay calculated during "hearbeat".
type client struct {
	// id is stored to identify the "User model" that stands behind the
	// connection.
	id   string
	conn *websocket.Conn
	// Buffered channel of outbound messages. Used to prevent race condition
	// between multiple concurrent writers. The "gorilla/websocket"
	// package allows only one concurrent writer at time.
	send chan []byte
	// Notify the server about disconnection.
	out chan *client
	// Network latency in milliseconds. It is reported by client so
	// shoudln't be trusted. Used only to render the UI connection bar.
	// In milliseconds.
	reportedLatency int
}

// initClient initializes the client, sets the connection properties,
// and runs the client's goroutines.
func initClient(id string, conn *websocket.Conn, out chan *client) *client {
	c := &client{
		id:   id,
		conn: conn,
		send: make(chan []byte, 256),
		out:  out,
	}

	c.conn.SetReadLimit(maxMessageSize)

	go c.read()
	go c.write()
	return c
}

func (c *client) read() {
	for {
		msgType, raw, err := c.conn.ReadMessage()
		if err != nil {
			// "gorilla/websocket" package allows to have "expected" error codes.
			// Those are often "CloseGoingAway" and "CloseAbnormalClosure".
			// This represents the case in which the client manually terminates
			// the connection by closing the browser tab.
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNoStatusReceived) {
				// A "real" error occurred. Might want to handle it.
				log.Printf("error: %v\n", err)
			}
			break
		}
		// Since all messages are JSON encoded, they must have text type.
		if msgType != websocket.TextMessage {
			log.Printf("client %s sends message of incorrect type: %d\n", c.id, msgType)
			break
		}

		var msg message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("client %s sends invalid message: %v\n", c.id, err)
			break
		}

		switch msg.Kind {
		// Immediately handle ping messages.
		case kindPing:
			if err := c.handlePing(msg.Payload); err != nil {
				break
			}
		default:
			c.send <- raw
		}
	}
	c.conn.Close()
}

func (c *client) write() {
	for {
		raw, ok := <-c.send
		if !ok {
			// The channel is closed. Terminate the connection.
			c.conn.WriteMessage(websocket.CloseMessage, nil)
			break
		}
		c.conn.SetWriteDeadline(time.Now().Add(writeWait))

		// If there are more than one message awaiting delivery,
		// group them into a single JSON array and send as a single package.
		// This helps to reduce the amount of memory allocations needed to
		// instantiate a lot of message writers.
		// It is important to not exceed the message size threshold.
		// If it can be exceeded, leave the rest of messages in queue for
		// better times.
		numMessages := len(c.send)
		if numMessages > 0 {
			pack := make([]json.RawMessage, 0, numMessages+1)
			pack = append(pack, raw)
			currSize := len(raw)
			for range numMessages {
				next := <-c.send
				pack = append(pack, next)
				currSize += len(next)
				if currSize >= maxMessageSize {
					break
				}
			}

			var err error
			raw, err = json.Marshal(pack)
			if err != nil {
				log.Printf("couldn't encode a pack of messages for client %s: %v\n", c.id, err)
				break
			}
		}

		w, err := c.conn.NextWriter(websocket.TextMessage)
		if err != nil {
			log.Printf("couldn't istantiate message writer for client %s: %v\n", c.id, err)
			break
		}
		_, err = w.Write(raw)
		if err != nil {
			log.Printf("couldn't write message for client %s: %v\n", c.id, err)
		}

		if err := w.Close(); err != nil {
			log.Printf("couldn't close message writer for client %s: %v\n", c.id, err)
			break
		}
	}
	// It's safe to close the connection multiple times.
	c.conn.Close()
	c.out <- c
}

func (c *client) handlePing(payload json.RawMessage) error {
	var latency int
	if err := json.Unmarshal(payload, &latency); err != nil {
		return err
	}
	c.reportedLatency = latency
	c.send <- nil
	return nil
}
