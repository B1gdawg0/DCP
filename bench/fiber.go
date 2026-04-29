package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
)

func runFiberServer() {
	app := fiber.New()

	app.Post("/user", func(c *fiber.Ctx) error {
		var req UserRequest
		if err := c.BodyParser(&req); err != nil {
			return c.Status(400).SendString("bad request")
		}

		resp := UserResponse{
			UserID: req.UserID,
			Name:   "John Doe",
			Score:  100,
		}

		return c.JSON(resp)
	})

	app.Listen(":8080")
}

func callREST() {
	reqBody, _ := json.Marshal(UserRequest{
		UserID: "user-123",
	})

	start := time.Now()

	resp, err := http.Post("http://localhost:8080/user", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	fmt.Println("REST latency:", time.Since(start))
	fmt.Println("REST response:", string(body))
}