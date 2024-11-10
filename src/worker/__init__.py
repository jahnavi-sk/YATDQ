from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import yaml
import logging
import json
from typing import Dict, Any, List, Callable

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaWorker:
    def __init__(self, config_path: str):
        """Initialize Kafka worker with configuration from YAML file.
        
        Args:
            config_path (str): Path to Kafka configuration YAML file
        """
        self.config = self._load_config(config_path)
        self.producer = None
        self.consumer = None
        self.is_running = False
        self.worker_id = None

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            raise

    def initialize(self, worker_id: str, task_topic: str, result_topic: str) -> None:
        """Initialize worker with Kafka connections.
        
        Args:
            worker_id (str): Unique identifier for this worker
            task_topic (str): Topic to consume tasks from
            result_topic (str): Topic to produce results to
        """
        self.worker_id = worker_id
        self._setup_producer()
        self._setup_consumer(worker_id, [task_topic])
        self.result_topic = result_topic
        logger.info(f"Worker {worker_id} initialized")

    def _setup_producer(self) -> None:
        """Initialize the Kafka producer for sending results."""
        producer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'client.id': f'worker-{self.worker_id}',
            'acks': 'all'
        }
        self.producer = Producer(producer_config)
        logger.info("Producer initialized")

    def _setup_consumer(self, group_id: str, topics: List[str]) -> None:
        """Initialize the Kafka consumer for receiving tasks.
        
        Args:
            group_id (str): Consumer group ID for load balancing
            topics (List[str]): Topics to subscribe to
        """
        consumer_config = {
            'bootstrap.servers': self.config['kafka']['bootstrap_servers'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe(topics)
        logger.info(f"Consumer subscribed to topics: {topics}")

    def start_processing(self, task_handler: Callable[[Dict[str, Any]], Any]) -> None:
        """Start processing tasks using the provided handler.
        
        Args:
            task_handler: Function to process tasks
        """
        if not self.consumer or not self.producer:
            raise RuntimeError("Worker not initialized. Call initialize() first.")

        self.is_running = True
        logger.info(f"Worker {self.worker_id} started processing tasks")

        try:
            while self.is_running:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse task message
                    task = json.loads(msg.value().decode('utf-8'))
                    task_id = task.get('task-id')
                    
                    # Update task status to processing
                    self._send_status_update(task_id, "processing")

                    # Process task
                    result = task_handler(task)

                    # Send success result
                    self._send_result(task_id, "success", result)
                    
                    # Commit offset after successful processing
                    self.consumer.commit(msg)
                    
                except Exception as e:
                    logger.error(f"Error processing task: {e}")
                    if task_id:
                        self._send_result(task_id, "failed", str(e))

        except KeyboardInterrupt:
            logger.info("Shutting down worker...")
        finally:
            self.stop()

    def _send_status_update(self, task_id: str, status: str) -> None:
        """Send task status update to result topic."""
        try:
            result = {
                "task-id": task_id,
                "status": status
            }
            self.producer.produce(
                self.result_topic,
                key=task_id.encode('utf-8'),
                value=json.dumps(result).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send status update: {e}")

    def _send_result(self, task_id: str, status: str, result: Any) -> None:
        """Send task result to result topic."""
        try:
            result_msg = {
                "task-id": task_id,
                "status": status,
                "result": result
            }
            self.producer.produce(
                self.result_topic,
                key=task_id.encode('utf-8'),
                value=json.dumps(result_msg).encode('utf-8'),
                callback=self._delivery_callback
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send result: {e}")

    def _delivery_callback(self, err, msg) -> None:
        """Callback for producer delivery reports."""
        if err:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def stop(self) -> None:
        """Stop the worker and clean up resources."""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info(f"Worker {self.worker_id} stopped")
