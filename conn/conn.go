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
	writer   *bufio.Writer
	reader   *bufio.Reader
	sendCh   chan *p.Frame
	inflight sync.Map
	dedup    *DedupStore
	handler  FrameHandler

	closeOnce sync.Once
	closeCh   chan struct{}
	writeMu   sync.Mutex
}

type FrameHandler func(frame *p.Frame) (*p.Frame, error)

func NewConn(raw net.Conn, handler FrameHandler) *Conn {
	c := &Conn{
		raw:     raw,
		reader:  bufio.NewReader(raw),
		writer:  bufio.NewWriterSize(raw, 64*1024),
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
			if err := c.writeDirect(frame); err != nil {
				return
			}
		}
	}
}

// writeDirect writes header then auth then payload separately — no full-frame allocation
func (c *Conn) writeDirect(frame *p.Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := p.EncodeHeader(c.writer, frame); err != nil {
		return err
	}
	if len(frame.Auth) > 0 {
		if _, err := c.writer.Write(frame.Auth); err != nil {
			return err
		}
	}
	if len(frame.Payload) > 0 {
		if _, err := c.writer.Write(frame.Payload); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

func (c *Conn) route(frame *p.Frame) {
	fmt.Println("debug received:", frame.Header.Type)

	switch frame.Header.Type {

	case p.TypeAccepted, p.TypeRejected:
		c.deliverToInFlight(frame)

	case p.TypeCompleted, p.TypeFailed, p.TypeCancelled, p.TypeExpired:
		key := string(frame.Header.RequestID[:])
		if c.dedup.CheckAndMark(key) {
			// Fix 3: only ACK if sender wants it
			if frame.Header.Flags&p.FlagNoACK == 0 {
				c.sendACK(frame)
			}
			return
		}
		c.deliverToInFlight(frame)
		if frame.Header.Flags&p.FlagNoACK == 0 {
			c.sendACK(frame)
		}

	case p.TypeRequest, p.TypeQuery:
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

// RegisterInFlight — Fix 4: store timer so it can be cancelled on completion
func (c *Conn) RegisterInFlight(entry *InFlight) {
	key := string(entry.RequestID[:])
	c.inflight.Store(key, entry)

	entry.timer = time.AfterFunc(time.Until(entry.Deadline), func() {
		if _, loaded := c.inflight.LoadAndDelete(key); loaded {
			entry.deliver(expiredFrame(entry.RequestID))
		}
	})
}