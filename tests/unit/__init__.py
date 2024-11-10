import unittest
from src.client import KafkaClient
from src.worker import KafkaWorker
from unittest.mock import patch, MagicMock
import json
class TestKafkaClient(unittest.TestCase):
    @patch('src.client.Producer')
    def test_produce_message(self, MockProducer):
        """Test producing a message to Kafka."""
        mock_producer_instance = MockProducer.return_value
        client = KafkaClient(config_path='config/kafka_config.yml')
        client.producer = mock_producer_instance

        # Produce a message
        client.produce_message('test_topic', b'test_value', key=b'test_key')

        # Assert that produce was called with the correct parameters
        mock_producer_instance.produce.assert_called_once_with(
            'test_topic', value=b'test_value', key=b'test_key'
        )

    @patch('src.worker.Consumer')
    def test_consume_messages(self, MockConsumer):
        """Test consuming messages from Kafka."""
        mock_consumer_instance = MockConsumer.return_value
        mock_consumer_instance.poll.return_value = MagicMock(value=lambda: b'{"task-id": "123", "status": "success"}')

        worker = KafkaWorker(config_path='config/kafka_config.yml')
        worker.consumer = mock_consumer_instance
        worker.initialize(worker_id='test_worker', task_topic='test_topic', result_topic='test_results')

        messages = list(worker.consume_messages(timeout=0.1))
        self.assertEqual(len(messages), 1)
        self.assertEqual(json.loads(messages[0].value().decode('utf-8'))['task-id'], '123')

    def test_task_submission(self):
        """Test task submission functionality."""
        client = KafkaClient(config_path='config/kafka_config.yml')
        task_id = client.submit_task('add', [1, 2])
        self.assertIsNotNone(task_id)

if __name__ == '__main__':
    unittest.main()