#!/bin/bash

# Set the Python virtual environment path if needed
# VENV_PATH="/path/to/your/venv"

# Activate the virtual environment if needed
# source "$VENV_PATH/bin/activate"

# Set Kafka broker address and topic
KAFKA_BROKER="localhost:9092"  # Change this to your Kafka broker address
TASK_TOPIC="task_topic"          # Change this to your actual task topic name

# Check if the worker script exists
if [ ! -f "worker.py" ]; then
    echo "Error: worker.py script not found!"
    exit 1
fi

# Run the worker script
echo "Starting the worker to consume tasks from ${TASK_TOPIC}..."
python worker.py --broker "${KAFKA_BROKER}" --topic "${TASK_TOPIC}"

# Optional: Deactivate the virtual environment if it was activated
# deactivate

echo "Worker has stopped."