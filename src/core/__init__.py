
from typing import Callable, Optional, Any, Dict
import json
import logging
from src.client import KafkaClient
from datetime import datetime

logger = logging.getLogger(__name__)

class MessageProcessor:
    """Handle message processing and transformation."""
    
    @staticmethod
    def serialize_message(data: Dict[str, Any]) -> bytes:
        """Serialize message data to JSON bytes."""
        try:
            data['timestamp'] = datetime.utcnow().isoformat()
            return json.dumps(data).encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to serialize message: {str(e)}")
            raise

    @staticmethod
    def deserialize_message(message: bytes) -> Dict[str, Any]:
        """Deserialize message bytes to dictionary."""
        try:
            return json.loads(message.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to deserialize message: {str(e)}")
            raise

class KafkaHandler:
    """High-level Kafka operations handler."""
    
    def __init__(self, config_path: str = "config/kafka_config.yml"):
        """Initialize KafkaHandler with a KafkaClient."""
        self.client = KafkaClient(config_path)
        self.processor = MessageProcessor()

    def publish_message(self, topic: str, data: Dict[str, Any], key: Optional[str] = None) -> None:
        """Publish a message to a Kafka topic.
        
        Args:
            topic (str): Target topic
            data (Dict[str, Any]): Message data
            key (Optional[str]): Message key
        """
        try:
            serialized_data = self.processor.serialize_message(data)
            serialized_key = key.encode('utf-8') if key else None
            self.client.produce_message(topic, serialized_data, serialized_key)
            logger.info(f"Successfully published message to topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            raise

    def process_messages(self, 
                        group_id: str, 
                        topics: list[str], 
                        handler_func: Callable[[Dict[str, Any]], None],
                        error_handler: Optional[Callable[[Exception], None]] = None):
        """Process messages from specified topics with a handler function.
        
        Args:
            group_id (str): Consumer group ID
            topics (list[str]): List of topics to subscribe to
            handler_func (Callable): Function to process each message
            error_handler (Optional[Callable]): Function to handle errors
        """
        try:
            self.client.create_consumer(group_id, topics)
            logger.info(f"Started consuming from topics: {topics}")
            
            for message in self.client.consume_messages():
                try:
                    data = self.processor.deserialize_message(message.value())
                    handler_func(data)
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    if error_handler:
                        error_handler(e)
                    else:
                        raise
                    
        except Exception as e:
            logger.error(f"Error in message processing loop: {str(e)}")
            raise
        finally:
            self.close()

    def close(self):
        """Clean up resources."""
        self.client.close()

class BatchProcessor(KafkaHandler):
    """Handle batch processing of messages."""
    
    def __init__(self, batch_size: int = 100, *args, **kwargs):
        """Initialize BatchProcessor with batch size."""
        super().__init__(*args, **kwargs)
        self.batch_size = batch_size
        self.current_batch: list[Dict[str, Any]] = []

    def process_batch(self, 
                     group_id: str, 
                     topics: list[str], 
                     batch_handler: Callable[[list[Dict[str, Any]]], None]):
        """Process messages in batches.
        
        Args:
            group_id (str): Consumer group ID
            topics (list[str]): List of topics to subscribe to
            batch_handler (Callable): Function to process each batch
        """
        def handle_message(message: Dict[str, Any]):
            self.current_batch.append(message)
            if len(self.current_batch) >= self.batch_size:
                batch_handler(self.current_batch)
                self.current_batch = []

        try:
            self.process_messages(group_id, topics, handle_message)
        finally:
            # Process any remaining messages in the batch
            if self.current_batch:
                batch_handler(self.current_batch)