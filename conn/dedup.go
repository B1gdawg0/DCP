package conn

import (
	"sync"
	"time"
)

type DedupStore struct {
    seen map[string]time.Time
    mu   sync.Mutex
    ttl  time.Duration
}

func NewDedupStore(ttl time.Duration) *DedupStore {
	d := &DedupStore{
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}

	go func() {
		ticker := time.NewTicker(ttl)
		defer ticker.Stop()

		for range ticker.C {
			d.Reap()
		}
	}()

	return d
}

func (d *DedupStore) IsDuplicate(requestID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	t, ok := d.seen[requestID]
	if !ok {
		return false
	}

	if time.Since(t) > d.ttl {
		delete(d.seen, requestID)
		return false
	}

	return true
}

func (d *DedupStore) Mark(requestID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.seen[requestID] = time.Now()
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