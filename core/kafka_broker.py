from kafka import KafkaProducer, KafkaConsumer
import json
import logging
from core.config.development import KAFKA_BROKER_URL

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaBroker:
    def __init__(self):
        # Initialize the Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER_URL,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
        )
        
        # Initialize the Kafka consumer
        self.consumer = KafkaConsumer(
            'task_queue',  # Topic to consume from
            bootstrap_servers=KAFKA_BROKER_URL,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON
            auto_offset_reset='earliest',  # Start from the earliest message
            group_id='task_consumers',  # Consumer group
            enable_auto_commit=True  # Automatically commit offsets
        )

    def send_message(self, message):
        """
        Sends a message to the Kafka topic.

        Parameters:
        - message: The message to send (should be a dictionary).
        """
        try:
            self.producer.send('task_queue', message)
            self.producer.flush()  # Ensure all messages are sent
            logger.info(f"Message sent to Kafka: {message}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")

    def consume_messages(self):
        """
        Consumes messages from the Kafka topic.

        Yields:
        - The consumed message.
        """
        try:
            for message in self.consumer:
                logger.info(f"Message received from Kafka: {message.value}")
                yield message.value
        except Exception as e:
            logger.error(f"Failed to consume messages from Kafka: {e}")

    def close(self):
        """Close the producer and consumer connections."""
        self.producer.close()
        self.consumer.close()
        logger.info("Kafka connections closed.")

if __name__ == "__main__":
    # Example usage of KafkaBroker
    kafka_broker = KafkaBroker()
    
    # Send a test message
    test_message = {
        "task-id": "test-task-id",
        "task": "add",
        "args": [1, 2]
    }
    kafka_broker.send_message(test_message)
    
    # Consume messages (this will run indefinitely)
    try:
        for msg in kafka_broker.consume_messages():
            print(f"Processed message: {msg}")
    except KeyboardInterrupt:
        kafka_broker.close()