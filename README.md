### Yer Another Task Distributed Queue

A distributed task queue system where clients can send asynchronous tasks to be processed by worker nodes. Tasks are first sent by the client to a message broker (Kafka), which acts as the central task queue. Workers retrieve tasks from this queue, execute them asynchronously, and store the results in a status/results database. Clients can then query the status/results database to fetch the outcome of the tasks once they have been processed. This architecture ensures task processing across distributed worker nodes.




##### for running it !

##### from scripts folder
- ```./run_worker.sh```
  
- ```./run_client.sh```
