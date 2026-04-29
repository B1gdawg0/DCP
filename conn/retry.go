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
	requestID := frame.Header.RequestID

	entry := NewInFlight(requestID, time.Now().Add(10*time.Second)) // ACK wait window
	c.RegisterInFlight(entry)

	go func() {
		defer c.removeInFlight(requestID)

		backoff := initialBackoff

		for attempt := 0; attempt < maxRetries; attempt++ {
			_ = c.Send(frame)

			select {
			case res, ok := <-entry.ResultCh:
				if !ok {
					return
				}

				if res.Header.Type == p.TypeACK {
					return
				}

			case <-time.After(backoff):
			}

			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		_ = c.Send(failedFrame(req, ErrRetryExceeded))
	}()
}