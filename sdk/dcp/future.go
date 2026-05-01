package dcp

import (
	"context"
	"fmt"

	"github.com/B1gdawg0/DCP/proto"
)

type Future struct {
	acceptedCh <-chan struct{}
	resultCh   <-chan *proto.Frame
	doneCh     chan struct{}
	result     *Result
}

type Result struct {
	Payload []byte
	Err     error
	Status  proto.MessageType
}

func (f *Future) Accepted(ctx context.Context) error {
	select {
	case <-f.acceptedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

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