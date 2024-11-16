#!/bin/bash

# Set the Kafka broker address and topic names
KAFKA_BROKER="localhost:9092"  # Change this to your Kafka broker address
TASK_TOPIC="task_queue"         # Change this to your actual task topic name

# Check if sample_tasks.json exists
if [ ! -f "sample_tasks.json" ]; then
    echo "Error: sample_tasks.json file not found!"
    exit 1
fi

# Activate the Python virtual environment if needed
# source /path/to/your/venv/bin/activate

# Set PYTHONPATH to include the root directory of the project (parent directory of client and core)
export PYTHONPATH=$(dirname "$(pwd)"):$PYTHONPATH

# Submit tasks via python3 -m
python3 - <<EOF
import json
from client.client import Client  # Ensure the client module is imported correctly

# Initialize the client
client = Client()

# Load tasks from the JSON file
with open('sample_tasks.json', 'r') as f:
    tasks = json.load(f)

# Submit each task
for task in tasks:
    task_name = task.get("task")
    args = task.get("args")
    if not task_name or args is None:
        print(f"Skipping invalid task: {task}")
        continue
    task_id = client.submit_task(task_name, args)
    print(f"Submitted task {task_name} with ID: {task_id}")

EOF

echo "All tasks have been submitted."

