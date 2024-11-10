
from  confluent_kafka import Producer, Consumer
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, config_path: str = "config/kafka_config.yml"):
        """Initialize Kafka client with configuration from YAML file.
        
        Args:
            config_path (str): Path to Kafka configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load Kafka configuration from YAML file."""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {str(e)}")
            raise

    def create_producer(self) -> Producer:
        """Create and return a Kafka producer instance."""
        if not self.producer:
            self.producer = Producer(self.config.get('producer', {}))
        return self.producer

    def create_consumer(self, group_id: str, topics: list[str]) -> Consumer:
        """Create and return a Kafka consumer instance.
        
        Args:
            group_id (str): Consumer group ID
            topics (list[str]): List of topics to subscribe to
        """
        if not self.consumer:
            consumer_config = self.config.get('consumer', {})
            consumer_config['group.id'] = group_id
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(topics)
        return self.consumer

    def produce_message(self, topic: str, value: bytes, key: Optional[bytes] = None) -> None:
        """Produce a message to a Kafka topic.
        
        Args:
            topic (str): Topic to produce to
            value (bytes): Message value
            key (bytes, optional): Message key
        """
        if not self.producer:
            self.create_producer()
        
        try:
            self.producer.produce(topic, value=value, key=key)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {str(e)}")
            raise

    def consume_messages(self, timeout: float = 1.0):
        """Consume messages from subscribed topics.
        
        Args:
            timeout (float): Maximum time to block waiting for message
        
        Yields:
            Message: Consumed message
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialized. Call create_consumer first.")

        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                yield msg
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            raise
        finally:
            self.close()

    def close(self):
        """Close both producer and consumer if they exist."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.producer = None
        
        if self.consumer:
            self.consumer.close()
            self.consumer = None