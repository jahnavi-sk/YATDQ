import time
import logging
import json
from kafka import KafkaConsumer, KafkaProducer  # Make sure to install kafka-python
from .task_handler import TaskHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, kafka_broker, task_topic, result_topic):
        """
        Initialize the Worker.

        Parameters:
        - kafka_broker: The Kafka broker address.
        - task_topic: The topic from which to consume tasks.
        - result_topic: The topic to which results will be sent.
        """
        self.consumer = KafkaConsumer(
            task_topic,
            bootstrap_servers=[kafka_broker],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        self.task_handler = TaskHandler()
        logger.info(f"Worker initialized and listening on topic: {task_topic}")

    def start(self):
        """Start processing tasks from the Kafka topic."""
        logger.info("Worker started.")
        for message in self.consumer:
            task = message.value
            logger.info(f"Received task: {task}")
            result = self.task_handler.handle_task(task)
            self.send_result(result)

    def send_result(self, result):
        """Send the result back to the result topic."""
        logger.info(f"Sending result: {result}")
        self.producer.send('result_topic', result)  # Change 'result_topic' to your actual topic name
        self.producer.flush()

# Example usage of the Worker class
if __name__ == "__main__":
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    task_topic = 'task_topic'          # Replace with your actual task topic name
    result_topic = 'result_topic'      # Replace with your actual result topic name

    worker = Worker(kafka_broker, task_topic, result_topic)
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker stopped.")