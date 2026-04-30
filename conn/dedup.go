package conn

import (
	"sync"
	"time"
)

type DedupStore struct {
    seen map[string]time.Time
    mu   sync.Mutex
    ttl  time.Duration
	stopCh chan struct{}
}

func NewDedupStore(ttl time.Duration) *DedupStore {
	d := &DedupStore{
		seen: make(map[string]time.Time),
		ttl:  ttl,
		stopCh: make(chan struct{}),
	}

	go func() {
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				d.Reap()
			case <-d.stopCh:
				return
			}
		}
	}()

	return d
}

func (d *DedupStore) Stop() {
	close(d.stopCh)
}

func (d *DedupStore) CheckAndMark(requestID string) (isDuplicate bool) {
    d.mu.Lock()
    defer d.mu.Unlock()
    t, ok := d.seen[requestID]
    if ok && time.Since(t) <= d.ttl {
        return true
    }
    d.seen[requestID] = time.Now()
    return false
}

func (d *DedupStore) Reap() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	for k, t := range d.seen {
		if now.Sub(t) > d.ttl {
			delete(d.seen, k)
		}
	}
}