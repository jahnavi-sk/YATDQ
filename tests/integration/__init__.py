import unittest
from src.client import KafkaClient
from src.worker import KafkaWorker
import time
import json

class TestYADTQIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the Kafka client and worker for integration tests."""
        cls.kafka_client = KafkaClient(config_path='config/kafka_config.yml')
        cls.kafka_worker = KafkaWorker(config_path='config/kafka_config.yml')
        cls.task_topic = 'yadtq_tasks'
        cls.result_topic = 'yadtq_results'
        cls.worker_id = 'test_worker_1'
        
        # Initialize the worker
        cls.kafka_worker.initialize(worker_id=cls.worker_id, task_topic=cls.task_topic, result_topic=cls.result_topic)

    def test_task_submission_and_processing(self):
        """Test the submission of a task and its processing by the worker."""
        task_id = self.kafka_client.submit_task('add', [1, 2])
        
        # Allow some time for the worker to process the task
        time.sleep(2)

        # Check the result
        result = self.kafka_worker.consume_messages(timeout=1.0)
        for msg in result:
            result_data = json.loads(msg.value().decode('utf-8'))
            if result_data['task-id'] == task_id:
                self.assertEqual(result_data['status'], 'success')
                self.assertEqual(result_data['result'], 3)  # Assuming the add function returns 3
                break
        else:
            self.fail("Task result not found in the results topic.")

    @classmethod
    def tearDownClass(cls):
        """Clean up resources after tests."""
        cls.kafka_worker.stop()

if __name__ == '__main__':
    unittest.main()