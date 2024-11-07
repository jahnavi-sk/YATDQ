package worker

import (
	"encoding/json"
	"log"
	"time"
	"yadtq/backend"
	"yadtq/broker"
)

type Worker struct {
	id      int
	broker  *broker.KafkaBroker
	backend *backend.RedisBackend
}

func StartWorker(id int, broker *broker.KafkaBroker, backend *backend.RedisBackend) {
	worker := &Worker{id: id, broker: broker, backend: backend}
	log.Printf("Worker %d started", id)
	worker.processTasks()
}

func (w *Worker) processTasks() {
	for {
		taskData, err := w.broker.GetTask()
		if err != nil {
			log.Printf("Worker %d: Error reading task: %v", w.id, err)
			continue
		}

		var task map[string]interface{}
		if err := json.Unmarshal(taskData, &task); err != nil {
			log.Printf("Worker %d: Error unmarshalling task: %v", w.id, err)
			continue
		}

		taskID := task["task_id"].(string)
		w.backend.SetTaskStatus(taskID, "processing")

		// Simulate task execution with a delay
		time.Sleep(2 * time.Second)

		// Assume a task type of "add" for simplicity
		args := task["args"].([]interface{})
		result := int(args[0].(float64)) + int(args[1].(float64))
		w.backend.SetTaskResult(taskID, string(result))
		w.backend.SetTaskStatus(taskID, "success")
	}
}
