[*YADTQ (Yet Another Distributed Task Queue)
Overview
YADTQ is a distributed task queue system designed to manage tasks across multiple worker nodes efficiently. Utilizing Kafka as the communication service, YADTQ facilitates asynchronous task processing, allowing clients to submit tasks and retrieve results seamlessly. This project aims to provide a scalable, fault-tolerant solution for executing tasks in a distributed environment.
Table of Contents
Goals
About Distributed Task Queues
Problem Statement
System Design
Key Components
Message/Task Broker (Kafka Broker)
Workers
Result Backend
Client
Requirements
Client Side Support
Worker Side Support
Demo
Installation
Usage
Testing
Contributing
License
Goals
The primary goal of YADTQ is to design and build a distributed task queue that coordinates between multiple workers in a highly distributed setup. This system will ensure efficient task management and execution across various nodes.
About Distributed Task Queues
A distributed task queue helps manage tasks across multiple computers instead of relying on a single machine. This setup improves efficiency and reliability, allowing tasks to be processed in the background and at different times. It is particularly useful for large applications that require reliable and efficient handling of numerous tasks.
Problem Statement
Develop a distributed task queue system where clients can send asynchronous tasks to be processed by worker nodes. Tasks are sent to a message broker (Kafka), which acts as the central queue. Workers retrieve tasks, execute them asynchronously, and store results in a status/results database. Clients can then query this database for task outcomes.
System Design
The architecture of YADTQ separates client and worker code from the core module, ensuring that users can interact with the system without needing to manage the internal queue mechanics. Communication between components is exclusively through the Kafka broker.
Key Components
Message/Task Broker (Kafka Broker)
The Kafka broker serves as the message transport, sending and receiving messages between clients (producers) and workers (consumers). Each task is represented as a JSON object containing information about the task type and arguments.
Workers
Primary Role: Workers retrieve and execute tasks from the Kafka broker independently, allowing concurrent processing.
Health Monitoring: Workers send periodic heartbeats to track their health and status, ensuring system reliability.
Result Backend
Stores task-related information such as status and final results. It supports four statuses: queued, processing, success, and failed. The data can be stored in various formats or storage systems.
Client
Clients submit tasks and query their statuses. They should receive feedback on task execution success or failure along with relevant results or error messages.
Requirements
Client Side Support
Request Handling: Generate a unique ID for each submitted task.
Status Tracking: Clients can query task status using the unique ID.
Worker Side Support
Worker Assignment: Implement robust logic for distributing tasks evenly among active workers.
Task Status Updates: Workers update task statuses as they progress.
Heartbeat Mechanism: Workers send periodic heartbeats to monitor their health.
Demo
To demonstrate multiple worker nodes:
Use multiple processes on different computers or VMs.
Feature at least three distinct task types (e.g., add, subtract, multiply).
Show that tasks are distributed across all worker nodes with parallel execution.
Installation
To set up YADTQ, follow these steps:
Clone the repository:
bash
git clone https://github.com/yourusername/yadtq.git
cd yadtq

Install dependencies:
bash
pip install -r requirements.txt

Set up Kafka:
Follow Kafka installation instructions to set up your Kafka broker.
Usage
Start the Kafka broker.
Run the worker nodes:
bash
./scripts/run_worker.sh

Use the client to submit tasks:
bash
./scripts/run_client.sh

Testing
Run tests using pytest:
bash
pytest tests/

Contributing
Contributions are welcome! Please fork the repository and create a pull request for any changes or enhancements.
License
This project is licensed under the MIT License - see the LICENSE file for details. This README provides an overview of YADTQ, its goals, architecture, components, usage instructions, and contribution guidelines, making it easier for developers to understand and work with the project effectively.
](https://www.perplexity.ai/search/give-me-a-project-structure-fo-wecAEGEiSf.9YUOaJraz5Q#4)
