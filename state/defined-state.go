package state

import "errors"

type State uint8

const (
	StatePending State = iota
	StateAccepted
	StateCompleted
	StateFailed
	StateCancelled
	StateExpired
)

var ErrInvalidTransition = errors.New("invalid transition")