# ⚡ DCP — Deferred Completion Protocol

> A binary protocol for **accept-now, complete-later** communication.

---

## 📌 What is DCP?

DCP is a transport-level protocol for long-running operations where:

* Receiver **accepts immediately**
* Work completes **asynchronously**
* Result is delivered later with **retry + confirmation**

---

## 🎯 Why DCP?

### HTTP (blocking)

```text
Client        Server
  |------------REQ------------>|
  |                            |
  |<-----------RESP------------|
```

```text
Client   |==== waiting =====================|
Server   |------ processing ---------------|
```

* Client blocked
* Result lost if connection drops

---

### DCP (non-blocking)

```text
Client        Server
  |---- REQUEST ---->|
  |<--- ACCEPTED ----|
  |                  |---- processing ----
  |                  |---- processing ----
  |                  |---- processing ----
  |<--- COMPLETED ---|
  |---- ACK -------->|
```

```text
Client   |-- short wait --|....free....|-- result --|
Server   |-- accept --|------ work ------|-- done --|
```

* Immediate acceptance
* Completion guaranteed via retry + ACK

---

## 🧱 Frame

```text
┌─────────────────────────────────────────────────┐
│  Magic (2B) │ Version (1B) │ Message Type (1B)  │
├─────────────────────────────────────────────────┤
│  Message ID (16B)                               │
│  Request ID (16B)  │  Correlation ID (16B)      │
├─────────────────────────────────────────────────┤
│  Sender (8B)  │  Timestamp (8B)                 │
│  Deadline (8B) │  Flags (2B)                    │
├─────────────────────────────────────────────────┤
│  Service (4B) │ Operation (4B) │ Version (2B)   │
│  Auth Len (2B) │ Payload Len (4B)               │
│  Checksum (4B)                                  │
├─────────────────────────────────────────────────┤
│  Auth Token (variable)                          │
│  Payload (variable)                             │
└─────────────────────────────────────────────────┘
```

---

## 🔄 Types

```text
REQUEST → ACCEPTED / REJECTED 
        → COMPLETED / FAILED / CANCELLED / EXPIRED → ACK
```

---

## 🔁 Retry Rules

```text
No ACCEPTED  → retry REQUEST
No ACK       → retry COMPLETED
Duplicate ID → return cached result
```

---

## ⏱ After TTL Deadline

```text
Caller:   stop waiting after Deadline
Receiver: send EXPIRED if exceeded
```

---

## 📦 Guarantees

```text
✔ At-least-once delivery
✔ Idempotent execution (MessageID)
✔ Completion guarantee (ACK)
```

```text
✘ No ordering
✘ No exactly-once
```

---

DCP turns unreliable networks into **reliable completion delivery**.
