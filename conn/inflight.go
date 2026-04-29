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
    ResultCh   chan *proto.Frame 
    SentAt     time.Time
    Deadline   time.Time
    RetryCount int
    mu         sync.Mutex

    once sync.Once
}

func NewInFlight(requestID [16]byte, deadline time.Time) *InFlight {
	return &InFlight{
		RequestID: requestID,
		Deadline:  deadline,
		ResultCh:  make(chan *proto.Frame, 1),
	}
}

func (i *InFlight) deliver(frame *proto.Frame) {
	switch frame.Header.Type {

	case proto.TypeAccepted:
		return

	default:
		i.once.Do(func() {
			select {
			case i.ResultCh <- frame:
			default:
			}
			close(i.ResultCh)
		})
	}
}