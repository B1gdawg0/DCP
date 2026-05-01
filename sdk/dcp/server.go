package dcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/B1gdawg0/DCP/conn"
	"github.com/B1gdawg0/DCP/proto"
	"github.com/hashicorp/yamux"
)

type Server struct {
	addr     string
	registry *registry
	listener net.Listener
}

func NewServer(addr string) *Server {
	return &Server{
		addr:     addr,
		registry: newRegistry(),
	}
}

func (s *Server) Handle(service, operation string, version uint8, fn HandlerFunc) {
	s.registry.register(service, operation, version, fn)
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = l
	defer l.Close()

	fmt.Println("dcp: server listening on", s.addr)

	go func() {
		for {
			rawTCP, err := l.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					fmt.Println("dcp: accept error:", err)
					continue
				}
			}

			session, err := yamux.Server(rawTCP, nil)
			if err != nil {
				rawTCP.Close()
				continue
			}
			go s.handleYamuxSession(ctx, session)
		}
	}()

	<-ctx.Done()
	return nil
}

func (s *Server) handleYamuxSession(ctx context.Context, session *yamux.Session) {
	for {
		stream, err := session.Accept()
		if err != nil {
			return
		}

		c := conn.NewConn(stream, s.dispatch)
		c.Start(ctx)
	}
}

func (s *Server) dispatch(frame *proto.Frame) (*proto.Frame, error) {
	fn, ok := s.registry.resolve(
		frame.Header.ServiceID,
		frame.Header.OpID,
		frame.Header.Version,
	)
	if !ok {
		return nil, ErrNoHandler
	}

	req := requestFromFrame(frame)
	resp, err := fn(req)
	if err != nil {
		return nil, err
	}

	return &proto.Frame{
		Header: proto.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   frame.Header.Version,
			Type:      proto.TypeCompleted,
			MessageID: frame.Header.MessageID,
			RequestID: frame.Header.RequestID,
			SenderID:  frame.Header.SenderID,
			Timestamp: time.Now().UnixNano(),
			Deadline:  frame.Header.Deadline,
			ServiceID: frame.Header.ServiceID,
			OpID:      frame.Header.OpID,
			Flags:     resp.Flags,
		},
		Payload: resp.Payload,
	}, nil
}