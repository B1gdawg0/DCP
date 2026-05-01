package dcp

import (
	"context"
	"net"
	"time"

	"github.com/B1gdawg0/DCP/conn"
	"github.com/B1gdawg0/DCP/proto"
	"github.com/google/uuid"
	"github.com/hashicorp/yamux"
)

type Client struct {
	session     *yamux.Session
	controlConn *conn.Conn
	dataConn    *conn.Conn
	senderID    [8]byte
	version     uint8
}

func NewClient(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	rawTCP, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	session, err := yamux.Client(rawTCP, nil)
	if err != nil {
		return nil, err
	}

	stream1, err := session.Open()
	if err != nil {
		return nil, err
	}
	controlConn := conn.NewConn(stream1, nil)

	stream2, err := session.Open()
	if err != nil {
		return nil, err
	}
	dataConn := conn.NewConn(stream2, nil)

	cl := &Client{
		session:     session,
		controlConn: controlConn,
		dataConn:    dataConn,
		version:     1,
	}

	cl.controlConn.Start(ctx)
	cl.dataConn.Start(ctx)

	return cl, nil
}

func (cl *Client) Close() {
	cl.session.Close()
}

func (cl *Client) Send(ctx context.Context, req *ClientRequest) (*Future, error) {
	var activeConn *conn.Conn
	if len(req.Payload) > 1024*1024 {
		activeConn = cl.dataConn
	} else {
		activeConn = cl.controlConn
	}

	frame, entry := cl.buildFrame(req)
	activeConn.RegisterInFlight(entry)

	if err := activeConn.Send(frame); err != nil {
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