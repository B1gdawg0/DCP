package dcp

import (
	"time"

	"github.com/B1gdawg0/DCP/proto"
)

type Request struct {
	Service   string
	Operation string
	Version   uint8
	Payload   []byte
	Auth      []byte
	Deadline  time.Time
	raw       *proto.Frame
}

type Response struct {
	Payload []byte
	Flags   uint16
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