import time
import logging
import json
from kafka import KafkaConsumer, KafkaProducer  # Make sure to install kafka-python
from worker.task_handler import TaskHandler
from core.result_backend import ResultBackend 
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_topic(broker, topic_name, num_partitions=3, replication_factor=1):
    	"""
    	Create a Kafka topic programmatically.

    	Parameters:
    	- broker: The Kafka broker address (e.g., 'localhost:9092').
    	- topic_name: Name of the topic to create.
    	- num_partitions: Number of partitions for the topic.
    	- replication_factor: Replication factor for the topic (usually 1 for local development).
    	"""
    	admin_client = KafkaAdminClient(bootstrap_servers=[broker])

    	topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    	)

    	try:
        	admin_client.create_topics([topic])
        	logger.info(f"Topic '{topic_name}' created with {num_partitions} partitions.")
    	except TopicAlreadyExistsError:
        	logger.warning(f"Topic '{topic_name}' already exists.")
    	except Exception as e:
        	logger.error(f"Failed to create topic '{topic_name}': {e}")
    	finally:
        	admin_client.close()
        	
        	
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
            group_id="worker_group",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            
            enable_auto_commit=True,
            #consumer_timeout_ms=1000,
            #max_poll_records=1,
            #client_id=f"worker-{id(self)}",
            #partition_assignment_strategy=['org.apache.kafka.clients.consumer.RoundRobinAssignor'],

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

    create_topic(kafka_broker, task_topic, num_partitions=3, replication_factor=1)

    result_backend = ResultBackend()
    worker = Worker(kafka_broker, task_topic, result_backend)
    
    try:
        worker.start()
    except KeyboardInterrupt:
        logger.info("Worker stopped.")
