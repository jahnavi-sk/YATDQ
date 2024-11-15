#!/bin/bash

# Set the Kafka broker address and topic names
KAFKA_BROKER="localhost:9092"  # Change this to your Kafka broker address
TASK_TOPIC="task_queue"          # Change this to your actual task topic name

# Check if sample_tasks.json exists
if [ ! -f "sample_tasks.json" ]; then
    echo "Error: sample_tasks.json file not found!"
    exit 1
fi

# Activate the Python virtual environment if needed
# source /path/to/your/venv/bin/activate

# Run the Python script to send tasks to Kafka
python - <<EOF
import json
from kafka import KafkaProducer

# Load tasks from the JSON file
with open('sample_tasks.json', 'r') as f:
    tasks = json.load(f)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='${KAFKA_BROKER}',
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

# Send tasks to the Kafka topic
for task in tasks:
    producer.send('${TASK_TOPIC}', task)

producer.flush()
print("All tasks have been sent to ${TASK_TOPIC}.")
EOF

# Optional: Deactivate the virtual environment if it was activated
# deactivate

echo "Client script completed."
