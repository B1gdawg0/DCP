# DCP — Deferred Completion Protocol

A lightweight binary protocol for machine-to-machine communication where the server accepts a request immediately and delivers the result later. Built for systems where operations are slow, failures are transient, and double-execution is catastrophic.

---

## Why DCP

Standard HTTP/gRPC forces the caller to block until the server finishes. Under load, this means:

- Threads pile up waiting on slow downstream calls
- A 2-second DB bottleneck cascades into timeouts for every caller
- Retry logic is ad-hoc and risks double-execution

DCP separates the contract into two explicit phases:

```
Caller                        Server
  |------- REQUEST ----------->|
  |<------ ACCEPTED -----------|   ← caller is free immediately
  |                            |   ← server calls DB, 3rd-party, etc.
  |<------ COMPLETED ----------|   ← result arrives when ready
  |------- ACK --------------->|
```

The caller gets `ACCEPTED` in microseconds and can do meaningful work — prefetch data, warm a cache, process other requests — while the server handles the slow operation.

---

## Installation

```bash
go get github.com/B1gdawg0/DCP
```

Requires Go 1.21+.

---

## Quick Start

### Server

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/B1gdawg0/DCP/sdk/dcp"
)

func main() {
    server := dcp.NewServer("localhost:9000")

    server.Handle("payment", "authorize", 1, func(req *dcp.Request) (*dcp.Response, error) {
        // slow work here — caller already got ACCEPTED and moved on
        time.Sleep(300 * time.Millisecond)
        return &dcp.Response{
            Payload: []byte(`{"status":"authorized"}`),
        }, nil
    })

    log.Fatal(server.ListenAndServe(context.Background()))
}
```

### Client — async (recommended)

```go
client, err := dcp.NewClient(ctx, "localhost:9000")
if err != nil {
    log.Fatal(err)
}
defer client.Close()

future, err := client.Send(ctx, &dcp.ClientRequest{
    Service:   "payment",
    Operation: "authorize",
    Version:   1,
    Payload:   []byte(`{"amount":100}`),
    Deadline:  10 * time.Second,
})
if err != nil {
    log.Fatal(err)
}

// blocks until ACCEPTED, then runs doWork(), then blocks for COMPLETED
result, err := future.WaitAcceptedThenDo(ctx, func() {
    // this runs while the server is still processing
    prefetchUserProfile()
    warmProductCache()
})
```

### Client — sync (drop-in RPC replacement)

```go
result, err := client.Call(ctx, &dcp.ClientRequest{
    Service:   "payment",
    Operation: "authorize",
    Version:   1,
    Payload:   []byte(`{"amount":100}`),
    Deadline:  10 * time.Second,
})
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(result.Payload))
```

---

## Core Concepts

### Phases of a request

Every DCP request goes through a defined state machine. The caller only ever sees two things: a receipt and a result.

```
PENDING → ACCEPTED → COMPLETED
                   → FAILED
                   → EXPIRED
                   → CANCELLED
         REJECTED  (server refused immediately)
```

| State | Meaning |
|---|---|
| `ACCEPTED` | Server received the request and takes responsibility for it |
| `REJECTED` | Server refused — no handler registered, auth failed, etc. |
| `COMPLETED` | Work finished successfully, payload contains the result |
| `FAILED` | Handler returned an error |
| `EXPIRED` | Deadline elapsed before the handler finished |
| `CANCELLED` | Request was cancelled by either side |

### Service routing

Requests are routed by `service + operation + version`. The SDK maps string names to stable numeric IDs using FNV hashing — no configuration file needed.

```go
// these two are equivalent
server.Handle("payment", "authorize", 2, handler)

// on the client, use the same strings
client.Send(ctx, &dcp.ClientRequest{
    Service:   "payment",
    Operation: "authorize",
    Version:   2,
})
```

Version is a `uint8` — use it to run multiple versions of the same operation simultaneously during rollouts.

### Deadline

Every request carries a deadline (TTL). If the server does not complete the work before the deadline, it sends `EXPIRED` and the caller receives `ErrDeadlineExceeded`. The client also enforces the deadline locally — whichever fires first wins.

```go
&dcp.ClientRequest{
    Deadline: 5 * time.Second, // server has 5 seconds to complete
}
```

### Idempotency

DCP provides **wire-level idempotency** automatically. If a `COMPLETED` frame is lost and the server retries, the client's dedup store recognises the duplicate by `RequestID` and drops it — the handler is never called twice for the same delivery.

**Application-level idempotency** (e.g. preventing a payment from being charged twice if the client sends two separate requests) is the handler's responsibility.

---

## API Reference

### Server

```go
// NewServer creates a server that will listen on addr.
func NewServer(addr string) *Server

// Handle registers a handler for a service/operation/version route.
// Panics if called after ListenAndServe.
func (s *Server) Handle(service, operation string, version uint8, fn HandlerFunc)

// ListenAndServe starts accepting connections. Blocks until ctx is cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error
```

#### HandlerFunc

```go
type HandlerFunc func(req *Request) (*Response, error)

type Request struct {
    Service   string
    Operation string
    Version   uint8
    Payload   []byte
    Auth      []byte
    Deadline  time.Time
}

type Response struct {
    Payload []byte
    Flags   uint16 // optional: e.g. proto.FlagNoACK for large payloads
}
```

Returning a non-nil `error` from a handler sends `FAILED` to the caller with the error message as payload.

---

### Client

```go
// NewClient dials addr and returns a ready client.
func NewClient(ctx context.Context, addr string) (*Client, error)

func (c *Client) Close()

// Send is the async path. Returns a Future immediately after sending.
// The caller must call Accepted(), Wait(), or WaitAcceptedThenDo() on the Future.
func (c *Client) Send(ctx context.Context, req *ClientRequest) (*Future, error)

// Call is the sync path. Blocks until COMPLETED/FAILED/EXPIRED arrives.
func (c *Client) Call(ctx context.Context, req *ClientRequest) (*Result, error)
```

#### ClientRequest

```go
type ClientRequest struct {
    Service   string
    Operation string
    Version   uint8
    Payload   []byte
    Auth      []byte
    Deadline  time.Duration // how long the server has, e.g. 10*time.Second
}
```

---

### Future

`Future` is returned by `client.Send()`. It gives you explicit control over the two-phase async model.

```go
// Accepted blocks until the server sends ACCEPTED or ctx is cancelled.
// After this returns, the caller is free to do other work.
func (f *Future) Accepted(ctx context.Context) error

// Wait blocks until the final result arrives.
func (f *Future) Wait(ctx context.Context) (*Result, error)

// WaitAcceptedThenDo is the canonical DCP pattern:
// block for ACCEPTED → run doWork() → block for final result.
func (f *Future) WaitAcceptedThenDo(ctx context.Context, doWork func()) (*Result, error)

// OnComplete registers a callback and returns immediately.
// The callback fires in a goroutine when the result arrives.
func (f *Future) OnComplete(fn func(*Result))
```

#### Result

```go
type Result struct {
    Payload []byte
    Err     error
    Status  proto.MessageType // TypeCompleted, TypeFailed, TypeExpired, TypeCancelled
}
```

---

## Patterns

### Fire and forget with callback

```go
future, _ := client.Send(ctx, req)
future.OnComplete(func(result *dcp.Result) {
    if result.Err != nil {
        log.Println("failed:", result.Err)
        return
    }
    log.Println("done:", string(result.Payload))
})
// execution continues immediately here
```

### Prefetch while server processes

```go
future, _ := client.Send(ctx, &dcp.ClientRequest{
    Service:   "report",
    Operation: "generate",
    Version:   1,
    Payload:   reportParams,
    Deadline:  30 * time.Second,
})

result, err := future.WaitAcceptedThenDo(ctx, func() {
    // server is generating the report — use this time usefully
    userProfile = fetchUserProfile(userID)
    permissions = loadPermissions(userID)
})
```

### Fan-out — multiple concurrent requests

```go
futures := make([]*dcp.Future, len(items))
for i, item := range items {
    futures[i], _ = client.Send(ctx, &dcp.ClientRequest{
        Service:   "inventory",
        Operation: "reserve",
        Version:   1,
        Payload:   item,
        Deadline:  5 * time.Second,
    })
}

// collect all results
for i, f := range futures {
    result, err := f.Wait(ctx)
    // handle result[i]
}
```

### Version rollout

```go
// old handler still running on version 1
server.Handle("payment", "authorize", 1, oldHandler)

// new handler on version 2 — deploy gradually
server.Handle("payment", "authorize", 2, newHandler)
```

---

## Wire Format

DCP uses a fixed 96-byte binary header followed by variable-length auth and payload. No path, no method string, no ASCII headers.

```
┌─────────────────────────────────────────────────┐
│  Magic (2B) │ Version (1B) │ Message Type (1B)  │
├─────────────────────────────────────────────────┤
│  Message ID (16B)                               │
│  Request ID (16B)  │  Correlation ID (16B)      │
├─────────────────────────────────────────────────┤
│  Sender (8B)  │  Timestamp (8B)                 │
│  Deadline (8B) │  Flags (2B)                    │
├─────────────────────────────────────────────────┤
│  Service (4B) │ Operation (4B)                  │
│  Auth Len (2B) │ Payload Len (4B)               │
│  Checksum (4B)                                  │
├─────────────────────────────────────────────────┤
│  Auth Token (variable)                          │
│  Payload (variable)                             │
└─────────────────────────────────────────────────┘
```

Message types: `REQUEST=1 QUERY=2 ACCEPTED=3 REJECTED=4 COMPLETED=5 FAILED=6 CANCELLED=7 EXPIRED=8 ACK=9`

Payload encoding is left to the application — JSON, protobuf, and msgpack all work. For large payloads, set `proto.FlagNoACK` in `Response.Flags` to skip the ACK round-trip.

---

## Performance

Benchmarked against HTTP/1.1 on the same machine, same payload, persistent connections:

| Payload | DCP | HTTP/1.1 | Note |
|---|---|---|---|
| 1 KB | ~280µs | ~700µs | Binary header vs ASCII overhead |
| 10 KB | ~430µs | ~750µs | DCP leads |
| 5 MB | ~6.5ms | ~6.5ms | Equal after buffer pooling |

DCP's primary advantage is not raw throughput — it is **caller utilisation under slow handlers**. In a system where handlers average 300ms, HTTP blocks a thread for 300ms per request. DCP blocks for ~200µs (the ACCEPTED round-trip) and frees the caller to do other work.