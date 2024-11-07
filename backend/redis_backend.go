package backend

import (
	"context"
)

type RedisBackend struct {
	client *redis.Client
}

func NewRedisBackend(addr string) (*RedisBackend, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	return &RedisBackend{client: client}, nil
}

func (rb *RedisBackend) SetTaskStatus(taskID, status string) error {
	return rb.client.Set(context.Background(), taskID+":status", status, 0).Err()
}

func (rb *RedisBackend) GetTaskStatus(taskID string) (string, error) {
	return rb.client.Get(context.Background(), taskID+":status").Result()
}

func (rb *RedisBackend) SetTaskResult(taskID, result string) error {
	return rb.client.Set(context.Background(), taskID+":result", result, 0).Err()
}

func (rb *RedisBackend) GetTaskResult(taskID string) (string, error) {
	return rb.client.Get(context.Background(), taskID+":result").Result()
}
