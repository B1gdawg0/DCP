package conn

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	p "github.com/B1gdawg0/DCP/proto"
)

type Conn struct {
	raw      net.Conn
	reader   *bufio.Reader
	sendCh   chan *p.Frame
	inflight sync.Map
	dedup    *DedupStore
	handler  FrameHandler

	closeOnce sync.Once
	closeCh   chan struct{}
}

type FrameHandler func(frame *p.Frame) (*p.Frame, error)

func NewConn(raw net.Conn, handler FrameHandler) *Conn {
	c := &Conn{
		raw:     raw,
		reader:  bufio.NewReader(raw),
		sendCh:  make(chan *p.Frame, 256),
		dedup:   NewDedupStore(2 * time.Minute),
		handler: handler,
		closeCh: make(chan struct{}),
	}
	return c
}

func (c *Conn) Start(ctx context.Context) {
	go c.readLoop(ctx)
	go c.writeLoop(ctx)
	// go c.deadlineLoop(ctx)
}

func (c *Conn) Send(frame *p.Frame) error {
	select {
	case c.sendCh <- frame:
		return nil
	case <-c.closeCh:
		return ErrConnClosed
	}
}

func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.closeCh)
		c.raw.Close()
		c.dedup.Stop()
	})
}

func (c *Conn) readLoop(ctx context.Context) {
	defer c.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		default:
		}

		frame, err := p.DecodeFrame(c.reader)
		if err != nil {
			// connection broken or closed — stop
			fmt.Println("decode error:", err)
			return
		}

		c.route(frame)
	}
}

func (c *Conn) writeLoop(ctx context.Context) {
	defer c.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		case frame := <-c.sendCh:
			data, err := p.EncodeFrame(frame)
			if err != nil {
				fmt.Println("encode error:", err)
				continue // bad frame, skip it
			}
			if _, err := c.raw.Write(data); err != nil {
				fmt.Println("write error:", err)
				return // connection broken
			}
		}
	}
}

func (c *Conn) route(frame *p.Frame) {
	fmt.Println("received:", frame.Header.Type)

	switch frame.Header.Type {

	case p.TypeAccepted, p.TypeRejected:
		c.deliverToInFlight(frame)

	case p.TypeCompleted, p.TypeFailed, p.TypeCancelled, p.TypeExpired:
		requestID := frame.Header.RequestID

		key := string(requestID[:])
		if c.dedup.CheckAndMark(key) {
			c.sendACK(frame)
			return
		}

		c.deliverToInFlight(frame)
		c.sendACK(frame)

	case p.TypeRequest:
		if c.handler != nil {
			go c.handleRequest(frame)
		}

	case p.TypeQuery:
		if c.handler != nil {
			go c.handleRequest(frame)
		}

	case p.TypeACK:
		key := "ack:" + string(frame.Header.RequestID[:])
		if val, ok := c.inflight.Load(key); ok {
			ch := val.(chan struct{})
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func (c *Conn) handleRequest(req *p.Frame) {
	accepted := &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Header.Version,
			Type:      p.TypeAccepted,
			MessageID: req.Header.MessageID,
			RequestID: req.Header.RequestID,
			SenderID:  req.Header.SenderID,
			Timestamp: req.Header.Timestamp,
			Deadline:  req.Header.Deadline,
			ServiceID: req.Header.ServiceID,
			OpID:      req.Header.OpID,
		},
	}
	c.Send(accepted)

	result, err := c.handler(req)
	if err != nil {
		c.Send(failedFrame(req, err))
		return
	}

	if time.Now().After(time.Unix(0, req.Header.Deadline)) {
		c.Send(expiredFrame(req.Header.RequestID))
		return
	}

	c.sendWithRetry(result, req)
}

func (c *Conn) deliverToInFlight(frame *p.Frame) {
	key := string(frame.Header.RequestID[:])
	val, ok := c.inflight.Load(key)
	if !ok {
		return 
	}
	entry := val.(*InFlight)
	entry.deliver(frame)
}

func (c *Conn) sendACK(frame *p.Frame) {
	ack := &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   frame.Header.Version,
			Type:      p.TypeACK,
			MessageID: frame.Header.MessageID,
			RequestID: frame.Header.RequestID,
			SenderID:  frame.Header.SenderID,
			Timestamp: frame.Header.Timestamp,
			Deadline:  frame.Header.Deadline,
			ServiceID: frame.Header.ServiceID,
			OpID:      frame.Header.OpID,
		},
	}
	c.Send(ack)
}

func expiredFrame(requestID [16]byte) *p.Frame {
	return &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   1,
			Type:      p.TypeExpired,
			RequestID: requestID,
			Timestamp: time.Now().UnixNano(),
		},
	}
}

func failedFrame(req *p.Frame, err error) *p.Frame {
	return &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Header.Version,
			Type:      p.TypeFailed,
			MessageID: req.Header.MessageID,
			RequestID: req.Header.RequestID,
			SenderID:  req.Header.SenderID,
			Timestamp: time.Now().UnixNano(),
			Deadline:  req.Header.Deadline,
			ServiceID: req.Header.ServiceID,
			OpID:      req.Header.OpID,
		},
		Payload: []byte(err.Error()),
	}
}

func (c *Conn) RegisterInFlight(entry *InFlight) {
	key := string(entry.RequestID[:])

	c.inflight.Store(key, entry)

	timeout := time.Until(entry.Deadline)

	time.AfterFunc(timeout, func() {
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			entry.deliver(expiredFrame(entry.RequestID))
		}
	})
}

// func (c *Conn) removeInFlight(requestID [16]byte) {
// 	c.inflight.Delete(string(requestID[:]))
// }