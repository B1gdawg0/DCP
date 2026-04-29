package conn

import "errors"

var ErrConnClosed = errors.New("dcp: connection closed")
var ErrDeadlineExceeded = errors.New("dcp: deadline exceeded")
var ErrMaxRetries = errors.New("dcp: max retries reached")
var ErrRetryExceeded = errors.New("dcp: retry attempts exhausted")