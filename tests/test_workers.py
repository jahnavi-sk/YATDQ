import subprocess
import time
from kafka import KafkaProducer
import json
import os

def start_worker():
    """Start a worker subprocess."""
    worker_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..', 'worker', 'worker.py')

    try:
        return subprocess.Popen(
            ["python3", worker_path],
            cwd=os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),  # Ensure root directory
            env={**os.environ, "PYTHONPATH": os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))}  # Set PYTHONPATH
        )
    except Exception as e:
        print(f"Failed to start worker: {e}")
        raise

def send_test_tasks():
    """Send test tasks to the Kafka topic."""
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: json.dumps(m).encode('utf-8')
    )

    task_template = {
        'task-id': '',
        'task': 'add',
        'args': [1, 2]
    }

    for i in range(10):
        task = task_template.copy()
        task['task-id'] = f'task-{i+1}'
        try:
            producer.send('task_queue', task)
            print(f"Sent task: {task['task-id']}")
        except Exception as e:
            print(f"Failed to send task {task['task-id']}: {e}")
        time.sleep(1)  # Simulate task generation interval
    producer.flush()

def test_multiple_workers():
    """Test if multiple workers can work concurrently."""
    workers = []
    try:
        # Start workers
        workers = [start_worker() for _ in range(3)]
        print("Workers started...")

        # Send tasks
        send_test_tasks()

        # Wait for workers to process tasks
        time.sleep(10)

    except Exception as e:
        print(f"Error during testing: {e}")
    finally:
        # Terminate all workers
        for worker in workers:
            worker.terminate()
            worker.wait()  # Ensure cleanup
        print("Workers terminated.")

if __name__ == "__main__":
    test_multiple_workers()

