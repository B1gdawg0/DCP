package conn

import (
	"context"
	"net"
)

func Dial(ctx context.Context, address string, handler FrameHandler) (*Conn, error) {
	var d net.Dialer

	raw, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}

	return NewConn(raw, handler), nil
}