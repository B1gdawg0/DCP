package dcp

import (
	"context"
	"time"

	"github.com/B1gdawg0/DCP/conn"
	"github.com/B1gdawg0/DCP/proto"
	"github.com/google/uuid"
)

type Client struct {
	c        *conn.Conn
	senderID [8]byte
	version  uint8
}

func NewClient(ctx context.Context, addr string) (*Client, error) {
	c, err := conn.Dial(ctx, addr, nil)
	if err != nil {
		return nil, err
	}
	cl := &Client{c: c, version: 1}
	c.Start(ctx)
	return cl, nil
}

func (cl *Client) Close() { cl.c.Close() }

func (cl *Client) Send(ctx context.Context, req *ClientRequest) (*Future, error) {
	frame, entry := cl.buildFrame(req)
	cl.c.RegisterInFlight(entry)

	if err := cl.c.Send(frame); err != nil {
		return nil, err
	}

	return &Future{
		acceptedCh: entry.AcceptedCh,
		resultCh:   entry.ResultCh,
		doneCh:     make(chan struct{}),
	}, nil
}


func (cl *Client) Call(ctx context.Context, req *ClientRequest) (*Result, error) {
	future, err := cl.Send(ctx, req)
	if err != nil {
		return nil, err
	}
	return future.Wait(ctx)
}

type ClientRequest struct {
	Service   string
	Operation string
	Version   uint8
	Payload   []byte
	Auth      []byte
	Deadline  time.Duration
}

func (cl *Client) buildFrame(req *ClientRequest) (*proto.Frame, *conn.InFlight) {
	msgID := newID()
	reqID := newID()
	deadline := time.Now().Add(req.Deadline)

	frame := &proto.Frame{
		Header: proto.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Version,
			Type:      proto.TypeRequest,
			MessageID: msgID,
			RequestID: reqID,
			SenderID:  cl.senderID,
			Timestamp: time.Now().UnixNano(),
			Deadline:  deadline.UnixNano(),
			ServiceID: ServiceID(req.Service),
			OpID:      OperationID(req.Operation),
			Flags:     0,
		},
		Auth:    req.Auth,
		Payload: req.Payload,
	}

	entry := conn.NewInFlight(reqID, deadline)
	return frame, entry
}

func newID() [16]byte {
	return [16]byte(uuid.New())
}