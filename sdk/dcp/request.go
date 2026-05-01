package dcp

import (
	"time"

	"github.com/B1gdawg0/DCP/proto"
)

// Request is what a server-side handler receives
type Request struct {
	Service   string
	Operation string
	Version   uint8
	Payload   []byte
	Auth      []byte
	Deadline  time.Time
	raw       *proto.Frame
}

// Response is what a server-side handler returns
type Response struct {
	Payload []byte
	Flags   uint16 // e.g. proto.FlagNoACK for large payloads
}

func requestFromFrame(f *proto.Frame) *Request {
	return &Request{
		Version:  f.Header.Version,
		Payload:  f.Payload,
		Auth:     f.Auth,
		Deadline: time.Unix(0, f.Header.Deadline),
		raw:      f,
	}
}