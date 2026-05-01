package conn

import (
	"sync"
	"time"

	"github.com/B1gdawg0/DCP/proto"
	"github.com/B1gdawg0/DCP/state"
)

type InFlight struct {
	RequestID  [16]byte
	State      state.State
	AcceptedCh chan struct{}
	ResultCh   chan *proto.Frame
	SentAt     time.Time
	Deadline   time.Time
	RetryCount int
	timer      *time.Timer
	mu         sync.Mutex
	once       sync.Once
	acceptOnce sync.Once
}

func NewInFlight(requestID [16]byte, deadline time.Time) *InFlight {
	return &InFlight{
		RequestID:  requestID,
		Deadline:   deadline,
		AcceptedCh: make(chan struct{}),
		ResultCh:   make(chan *proto.Frame, 1),
	}
}

func (i *InFlight) deliver(frame *proto.Frame) {
	switch frame.Header.Type {
	case proto.TypeAccepted:
		i.acceptOnce.Do(func() {
			i.mu.Lock()
			i.State = state.StateAccepted
			i.mu.Unlock()
			close(i.AcceptedCh)
		})
	default:
		i.once.Do(func() {
			// Fix 4: cancel the deadline timer — request is done
			if i.timer != nil {
				i.timer.Stop()
			}
			select {
			case i.ResultCh <- frame:
			default:
			}
			close(i.ResultCh)
		})
	}
}