package main

import (
	"log"
	"time"
	"yadtq/backend"
	"yadtq/broker"
	"yadtq/client"
	"yadtq/worker"
)

func main() {
	// Initialize Redis Backend
	redisBackend, err := backend.NewRedisBackend("localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Initialize Kafka Broker
	kafkaBroker, err := broker.NewKafkaBroker("localhost:9092")
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}

	// Start Workers
	for i := 0; i < 3; i++ {
		go worker.StartWorker(i, kafkaBroker, redisBackend)
	}

	// Initialize Client and submit tasks
	client := client.NewClient(kafkaBroker, redisBackend)
	for i := 0; i < 5; i++ {
		client.SubmitTask("add", []int{i, i + 1})
	}

	// Periodic status checks
	for {
		time.Sleep(5 * time.Second)
		client.CheckAllTaskStatuses()
	}
}
