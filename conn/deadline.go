package conn

import (
	"context"
	"time"
)

func (c *Conn) deadlineLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now()

			c.inflight.Range(func(k, v any) bool {
				entry := v.(*InFlight)

				if now.After(entry.Deadline) {
					entry.deliver(expiredFrame(entry.RequestID))

					c.inflight.Delete(k)
				}
				return true
			})

		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		}
	}
}