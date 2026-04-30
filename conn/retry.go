package conn

import (
	"time"

	p "github.com/B1gdawg0/DCP/proto"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 2 * time.Second
	maxRetries     = 4
)

func (c *Conn) sendWithRetry(frame *p.Frame, req *p.Frame) {
    ackCh := make(chan struct{}, 1)
    key := "ack:" + string(frame.Header.RequestID[:])
    c.inflight.Store(key, ackCh) // separate namespace

    go func() {
        defer c.inflight.Delete(key)
        backoff := initialBackoff

        for attempt := 0; attempt < maxRetries; attempt++ {
            c.Send(frame)
            select {
            case <-ackCh:
                return // ACK received, done
            case <-time.After(backoff):
            }
            backoff *= 2
            if backoff > maxBackoff {
                backoff = maxBackoff
            }
        }
        c.Send(failedFrame(req, ErrRetryExceeded))
    }()
}