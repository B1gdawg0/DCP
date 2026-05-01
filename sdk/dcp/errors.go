package dcp

import "errors"

var ErrDeadlineExceeded = errors.New("dcp: deadline exceeded")
var ErrCancelled        = errors.New("dcp: request cancelled")
var ErrRejected         = errors.New("dcp: request rejected")
var ErrNoHandler        = errors.New("dcp: no handler registered for this service/operation/version")