package conn

import (
	"net"
)

type Listener struct {
	raw     net.Listener
	handler FrameHandler
}

func Listen(address string, handler FrameHandler) (*Listener, error) {
	raw, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	
	return &Listener{
		raw:     raw,
		handler: handler,
	}, nil
}

func (l *Listener) AcceptConn() (*Conn, error) {
	raw, err := l.raw.Accept()
	if err != nil {
		return nil, err
	}

	return NewConn(raw, l.handler), nil
}

func (l *Listener) Close() error {
	return l.raw.Close()
}