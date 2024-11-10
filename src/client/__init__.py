from confluent_kafka import Producer, Consumer
import yaml
import logging
import json
import uuid
from typing import Dict, Any, Optional, List

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
        self.task_topic = self.config.get('topics', {}).get('tasks', 'yadtq_tasks')
        self.result_topic = self.config.get('topics', {}).get('results', 'yadtq_results')

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
            producer_config = self.config.get('producer', {})
            producer_config.update({
                'client.id': f'yadtq-producer-{uuid.uuid4()}',
                'enable.idempotence': True,  # Ensure exactly-once delivery
                'acks': 'all'  # Wait for all replicas to acknowledge
            })
            self.producer = Producer(producer_config)
        return self.producer

    def create_consumer(self, group_id: str, topics: List[str]) -> Consumer:
        """Create and return a Kafka consumer instance.
        
        Args:
            group_id (str): Consumer group ID for load balancing
            topics (list[str]): List of topics to subscribe to
        """
        if not self.consumer:
            consumer_config = self.config.get('consumer', {})
            consumer_config.update({
                'group.id': group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,  # Manual commit for better control
                'max.poll.interval.ms': 300000  # 5 minutes max processing time
            })
            self.consumer = Consumer(consumer_config)
            self.consumer.subscribe(topics)
        return self.consumer

    def submit_task(self, task_type: str, args: List[Any], task_id: Optional[str] = None) -> str:
        """Submit a task to the task queue.
        
        Args:
            task_type (str): Type of task to execute
            args (list): Task arguments
            task_id (str, optional): Custom task ID, generated if not provided
            
        Returns:
            str: Task ID
        """
        if not self.producer:
            self.create_producer()
        
        task_id = task_id or str(uuid.uuid4())
        task = {
            'task_id': task_id,
            'task': task_type,
            'args': args
        }
        
        try:
            self.producer.produce(
                self.task_topic,
                value=json.dumps(task).encode('utf-8'),
                key=task_id.encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.flush()
            return task_id
        except Exception as e:
            logger.error(f"Failed to submit task {task_id}: {str(e)}")
            raise

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

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
                
                try:
                    # Process the message and commit offset only after successful processing
                    yield msg
                    self.consumer.commit(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    # Don't commit offset if processing failed
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