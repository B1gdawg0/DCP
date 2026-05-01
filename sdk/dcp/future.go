package dcp

import (
	"context"
	"fmt"

	"github.com/B1gdawg0/DCP/proto"
)

// Future is returned by Client.Send().
// The caller can use Wait() to block for the final result,
// or OnComplete() to register a callback and move on immediately.
type Future struct {
	acceptedCh <-chan struct{}    // closed when ACCEPTED arrives
	resultCh   <-chan *proto.Frame // closed when COMPLETED/FAILED/EXPIRED arrives
	doneCh     chan struct{}       // closed when callback has fired
	result     *Result
}

// Result is the final outcome of a DCP request
type Result struct {
	Payload []byte
	Err     error
	Status  proto.MessageType // TypeCompleted, TypeFailed, TypeExpired, TypeCancelled
}

// Accepted blocks until the server sends ACCEPTED or ctx is cancelled.
// This is the DCP sweet spot — after this returns you can do other work.
func (f *Future) Accepted(ctx context.Context) error {
	select {
	case <-f.acceptedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait blocks until the final result arrives (COMPLETED/FAILED/EXPIRED).
// This is the sync path — equivalent to a normal blocking RPC.
func (f *Future) Wait(ctx context.Context) (*Result, error) {
	select {
	case frame, ok := <-f.resultCh:
		if !ok {
			return f.result, nil
		}
		return resultFromFrame(frame), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// OnComplete registers a callback that fires when the result arrives.
// Returns immediately — the caller can move on.
func (f *Future) OnComplete(fn func(*Result)) {
	go func() {
		frame, ok := <-f.resultCh
		if !ok {
			if fn != nil {
				fn(f.result)
			}
			return
		}
		if fn != nil {
			fn(resultFromFrame(frame))
		}
		close(f.doneCh)
	}()
}

// WaitAcceptedThenDo is the canonical DCP pattern:
// 1. block until ACCEPTED
// 2. run doWork() while server is processing
// 3. block for final result
func (f *Future) WaitAcceptedThenDo(ctx context.Context, doWork func()) (*Result, error) {
	if err := f.Accepted(ctx); err != nil {
		return nil, err
	}
	if doWork != nil {
		doWork()
	}
	return f.Wait(ctx)
}

func resultFromFrame(frame *proto.Frame) *Result {
	r := &Result{Status: frame.Header.Type}
	switch frame.Header.Type {
	case proto.TypeCompleted:
		r.Payload = frame.Payload
	case proto.TypeFailed:
		r.Err = fmt.Errorf("dcp: remote failed: %s", frame.Payload)
	case proto.TypeExpired:
		r.Err = ErrDeadlineExceeded
	case proto.TypeCancelled:
		r.Err = ErrCancelled
	}
	return r
}