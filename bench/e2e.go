package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/B1gdawg0/DCP/conn"
	p "github.com/B1gdawg0/DCP/proto"
)

func setup(handler conn.FrameHandler) (*conn.Conn, *conn.Conn) {
	clientConn, serverConn := net.Pipe()

	server := conn.NewConn(serverConn, func(frame *p.Frame) (*p.Frame, error) {
		var req UserRequest
		_ = json.Unmarshal(frame.Payload, &req)

		time.Sleep(100 * time.Millisecond)

		respPayload, _ := json.Marshal(UserResponse{
			UserID: req.UserID,
			Name:   "John Doe",
			Score:  100,
		})

		return completedWithPayload(frame, respPayload), nil
	})
	client := conn.NewConn(clientConn, nil)

	ctx := context.Background()
	server.Start(ctx)
	client.Start(ctx)

	return client, server
}

func sendRequest(client *conn.Conn, timeout time.Duration) *p.Frame {
	reqID := [16]byte{1, 2, 3}

	entry := conn.NewInFlight(reqID, time.Now().Add(timeout))
	client.RegisterInFlight(entry)

	reqPayload, _ := json.Marshal(UserRequest{
		UserID: "user-123",
	})

	req := &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   1,
			Type:      p.TypeRequest,
			RequestID: reqID,
			MessageID: [16]byte{9, 9, 9},
			Timestamp: time.Now().UnixNano(),
			Deadline:  time.Now().Add(timeout).UnixNano(),
		},
		Payload: reqPayload,
	}

	_ = client.Send(req)
	return <-entry.ResultCh
}

func completed(req *p.Frame, msg string) *p.Frame {
	return &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Header.Version,
			Type:      p.TypeCompleted,
			MessageID: req.Header.MessageID,
			RequestID: req.Header.RequestID,
			Timestamp: time.Now().UnixNano(),
		},
		Payload: []byte(msg),
	}
}

func completedWithPayload(req *p.Frame, payload []byte) *p.Frame {
	return &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Header.Version,
			Type:      p.TypeCompleted,
			MessageID: req.Header.MessageID,
			RequestID: req.Header.RequestID,
			Timestamp: time.Now().UnixNano(),
		},
		Payload: payload,
	}
}

func rejected(req *p.Frame) *p.Frame {
	return &p.Frame{
		Header: p.Header{
			Magic:     [2]byte{0xDC, 0x50},
			Version:   req.Header.Version,
			Type:      p.TypeRejected,
			MessageID: req.Header.MessageID,
			RequestID: req.Header.RequestID,
			Timestamp: time.Now().UnixNano(),
		},
	}
}

func testRESTComparison() {
	fmt.Println("=== REST Comparison ===")

	// simple HTTP server
	go func() {
		http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(100 * time.Millisecond)
			w.Write([]byte("ok"))
		})
		http.ListenAndServe(":8080", nil)
	}()

	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	resp, _ := http.Get("http://localhost:8080/test")
	defer resp.Body.Close()

	fmt.Println("REST latency:", time.Since(start))
}

func testHighLoad() {
	fmt.Println("=== High Load ===")

	client, _ := setup(func(frame *p.Frame) (*p.Frame, error) {
		return completed(frame, "ok"), nil
	})

	start := time.Now()

	n := 5
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sendRequest(client, 2*time.Second)
		}(i)
	}

	wg.Wait()
	fmt.Println("total time:", time.Since(start))
}

func testRetry() {
	fmt.Println("=== Retry ===")

	client, _ := setup(func(frame *p.Frame) (*p.Frame, error) {
		time.Sleep(100 * time.Millisecond)
		return completed(frame, "retry"), nil
	})

	// simulate dropping ACK (comment sendACK temporarily)

	start := time.Now()
	res := sendRequest(client, 2*time.Second)
	fmt.Println("result:", res.Header.Type, "latency:", time.Since(start))
}

func testRejected() {
	fmt.Println("=== Rejected ===")

	client, _ := setup(func(frame *p.Frame) (*p.Frame, error) {
		return rejected(frame), nil
	})

	start := time.Now()
	res := sendRequest(client, 2*time.Second)
	fmt.Println("result:", res.Header.Type, "latency:", time.Since(start))
}

func testExpired() {
	fmt.Println("=== Expired ===")

	client, _ := setup(func(frame *p.Frame) (*p.Frame, error) {
		time.Sleep(500 * time.Millisecond) // slower than deadline
		return completed(frame, "late"), nil
	})

	start := time.Now()
	res := sendRequest(client, 100*time.Millisecond)
	fmt.Println("result:", res.Header.Type, "latency:", time.Since(start))
}

func testHappyPath() {
	fmt.Println("=== Happy Path ===")

	client, _ := setup(func(frame *p.Frame) (*p.Frame, error) {
		time.Sleep(100 * time.Millisecond)
		return completed(frame, "ok"), nil
	})

	start := time.Now()
	res := sendRequest(client, 2*time.Second)
	fmt.Println("result:", res.Header.Type, "latency:", time.Since(start))
}

// func main() {
// 	// testHappyPath()
// 	// testExpired()
// 	// testRejected()
// 	// testRetry()
// 	testHighLoad()

// 	// testRESTComparison()
// }