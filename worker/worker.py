import time
import logging
import json
from kafka import KafkaConsumer, KafkaProducer  # Make sure to install kafka-python
from worker.task_handler import TaskHandler
from core.result_backend import ResultBackend 

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, kafka_broker, task_topic, result_backend):
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
    
        self.task_handler = TaskHandler()
        self.result_backend = result_backend
        logger.info(f"Worker initialized and listening on topic: {task_topic}")

    def start(self):
        """Start processing tasks from the Kafka topic."""
        logger.info("Worker started.")
        for message in self.consumer:
            task = message.value
            logger.info(f"Received task: {task}")
            result = self.task_handler.handle_task(task)
            self.store_result(task['task-id'], result)
            #self.send_result(result)

    def store_result(self, task_id, result):
        """Store the result of the task in the backend (e.g., Redis)."""
        logger.info(f"Storing result for task {task_id}: {result}")
        self.result_backend.store_result(task_id, result['status'], result['result'])


    #def send_result(self, result):
        #"""Send the result back to the result topic."""
        #logger.info(f"Sending result: {result}")
        #self.producer.send('result_topic', result)  # Change 'result_topic' to your actual topic name
        #self.producer.flush()

# Example usage of the Worker class
if __name__ == "__main__":
    kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
    task_topic = 'task_queue'          # Replace with your actual task topic name
    result_topic = 'result_topic'      # Replace with your actual result topic name

    result_backend = ResultBackend()
    worker = Worker(kafka_broker, task_topic, result_backend)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker stopped.")
