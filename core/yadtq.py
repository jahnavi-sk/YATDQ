from core.kafka_broker import KafkaBroker
from core.result_backend import ResultBackend
from core.celery_app import app
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Initialize Kafka broker and result backend
    kafka_broker = KafkaBroker()
    result_backend = ResultBackend()

    # Example task to be sent to the Kafka broker
    task_id = "example-task-id"
    task = {
        "task-id": task_id,
        "task": "add",
        "args": [10, 20]
    }

    # Send the task to the Kafka broker
    kafka_broker.send_message(task)

    # Simulate processing and storing the result
    # In a real application, this would be handled by a Celery worker
    time.sleep(2)  # Simulate some processing time

    # Store the result in the result backend
    result_backend.store_result(task_id, "success", {"sum": 30})

    # Retrieve the result from the result backend
    result = result_backend.get_result(task_id)
    logger.info(f"Final result for task {task_id}: {result}")

    # Clean up
    kafka_broker.close()
    result_backend.close()

if __name__ == "__main__":
    main()