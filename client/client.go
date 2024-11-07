package client

import (
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"yadtq/backend"
	"yadtq/broker"
)

type Client struct {
	broker  *broker.KafkaBroker
	backend *backend.RedisBackend
}

type Task struct {
	ID   string `json:"task_id"`
	Type string `json:"task"`
	Args []int  `json:"args"`
}

func NewClient(broker *broker.KafkaBroker, backend *backend.RedisBackend) *Client {
	return &Client{broker: broker, backend: backend}
}

func (c *Client) SubmitTask(taskType string, args []int) {
	taskID := uuid.New().String()
	task := Task{ID: taskID, Type: taskType, Args: args}
	taskData, _ := json.Marshal(task)

	// Set initial task status in backend
	c.backend.SetTaskStatus(taskID, "queued")

	if err := c.broker.SubmitTask(taskData); err == nil {
		log.Printf("Submitted task %s", taskID)
	}
}

func (c *Client) CheckAllTaskStatuses() {
	for i := 0; i < 5; i++ {
		taskID := uuid.New().String()
		status, _ := c.backend.GetTaskStatus(taskID)
		result, _ := c.backend.GetTaskResult(taskID)
		log.Printf("Task %s: Status: %s, Result: %s", taskID, status, result)
	}
}
