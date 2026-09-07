package ws

import "encoding/json"

type messageKind int

const (
	kindPing messageKind = iota
	kindMove
)

type message struct {
	Kind    messageKind     `json:"k"`
	Payload json.RawMessage `json:"p"`
}

func mustEncode(k messageKind, p json.RawMessage) json.RawMessage {
	raw, err := json.Marshal(message{
		Kind:    k,
		Payload: p,
	})
	if err != nil {
		panic("cannot encode event")
	}
	return raw
}
