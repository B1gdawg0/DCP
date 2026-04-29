package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/B1gdawg0/DCP/conn"
	"github.com/B1gdawg0/DCP/proto"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run main.go [server|client|rest-server|rest-client]")
		return
	}

	switch os.Args[1] {
	case "server":
		runServer()

	case "client":
		runClient()

	case "rest-server":
		runFiberServer()

	case "rest-client":
		callREST()

	default:
		fmt.Println("unknown mode:", os.Args[1])
	}
}

func runClient() {
	raw, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		panic(err)
	}

	c := conn.NewConn(raw, nil)
	c.Start(context.Background())

	start := time.Now()
	res := sendRequest(c, 2*time.Second)

	fmt.Println("result:", res.Header.Type, "latency:", time.Since(start))
}

func runServer() {
	ln, err := net.Listen("tcp", ":9000")
	if err != nil {
		panic(err)
	}
	fmt.Println("server listening on :9000")

	for {
		connRaw, _ := ln.Accept()

		c := conn.NewConn(connRaw, func(frame *proto.Frame) (*proto.Frame, error) {
			var req UserRequest
			_ = json.Unmarshal(frame.Payload, &req)

			respPayload, _ := json.Marshal(UserResponse{
				UserID: req.UserID,
				Name:   "John Doe",
				Score:  100,
			})

			return completedWithPayload(frame, respPayload), nil
		})

		c.Start(context.Background())
	}
}