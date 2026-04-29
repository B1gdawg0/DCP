package main

type UserRequest struct {
	UserID string `json:"user_id"`
}

type UserResponse struct {
	UserID string `json:"user_id"`
	Name   string `json:"name"`
	Score  int    `json:"score"`
}