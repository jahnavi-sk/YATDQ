from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import yaml
import logging
import json
from typing import Dict, Any, List, Callable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.producer = None
        self.consumer = None
        self.is_running = False

    def setup_producer(self) -> None:
        """Initialize the Kafka producer"""
        producer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'client.id': self.config['kafka'].get('client_id', 'python-producer'),
            'acks': 'all'
        }
        self.producer = Producer(producer_config)
        logger.info("Kafka producer initialized")

    def setup_consumer(self, group_id: str, topics: List[str]) -> None:
        """Initialize the Kafka consumer"""
        consumer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe(topics)
        logger.info(f"Kafka consumer subscribed to topics: {topics}")

    def produce_message(self, topic: str, message: Dict[str, Any], key: str = None) -> None:
        """
        Produce a message to a Kafka topic
        
        Args:
            topic: The topic to produce to
            message: The message to produce (will be converted to JSON)
            key: Optional message key
        """
        if not self.producer:
            raise RuntimeError("Producer not initialized. Call setup_producer() first.")

        try:
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8') if key else None,
                value=json.dumps(message).encode('utf-8'),
                on_delivery=self._delivery_callback
            )
            # Trigger any available delivery callbacks
            self.producer.poll(0)
            
        except KafkaException as e:
            logger.error(f"Failed to produce message: {e}")
            raise

    def consume_messages(self, message_handler: Callable[[Dict[str, Any]], None], 
                        timeout: float = 1.0) -> None:
        """
        Start consuming messages and process them with the provided handler
        
        Args:
            message_handler: Callback function to process received messages
            timeout: Timeout in seconds for consumer poll
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialized. Call setup_consumer() first.")

        self.is_running = True
        
        try:
            while self.is_running:
                msg = self.consumer.poll(timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    message_handler(value)
                    self.consumer.commit(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
        finally:
            self.stop_consumer()

    def stop_consumer(self) -> None:
        """Stop the consumer and close the connection"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")

    def _delivery_callback(self, err, msg) -> None:
        """Callback for producer delivery reports"""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def __del__(self):
        """Cleanup when the object is destroyed"""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
