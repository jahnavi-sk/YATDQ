package broker

import (
	"log"
)

type KafkaBroker struct {
	writer *kafka.Writer
	reader *kafka.Reader
}

func NewKafkaBroker(brokerAddress string) (*KafkaBroker, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokerAddress),
		Topic:    "tasks",
		Balancer: &kafka.LeastBytes{},
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   "tasks",
		GroupID: "worker-group",
	})
	return &KafkaBroker{writer: writer, reader: reader}, nil
}

func (kb *KafkaBroker) SubmitTask(taskData []byte) error {
	err := kb.writer.WriteMessages(nil, kafka.Message{
		Value: taskData,
	})
	if err != nil {
		log.Printf("Failed to submit task to Kafka: %v", err)
	}
	return err
}

func (kb *KafkaBroker) GetTask() ([]byte, error) {
	msg, err := kb.reader.ReadMessage(nil)
	if err != nil {
		log.Printf("Failed to read task from Kafka: %v", err)
		return nil, err
	}
	return msg.Value, nil
}
