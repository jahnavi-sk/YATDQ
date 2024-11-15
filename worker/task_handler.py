import logging
from .utils import validate_task, serialize_to_json, deserialize_from_json, handle_error

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TaskHandler:
    def __init__(self):
        """Initialize the TaskHandler."""
        logger.info("TaskHandler initialized.")

    def handle_task(self, task):
        """
        Handle a task by validating and executing it.

        Parameters:
        - task: A dictionary representing the task.

        Returns:
        - dict: A dictionary containing the result of the task execution.
        """
        if not validate_task(task):
            handle_error("Invalid task.")
            return {"status": "error", "message": "Invalid task."}

        task_id = task['task-id']
        task_name = task['task']
        args = task['args']

        logger.info(f"Handling task: {task_id} - {task_name} with args: {args}")

        try:
            # Execute the task based on its name
            if task_name == "add":
                result = self.add(*args)
            elif task_name == "subtract":
                result = self.subtract(*args)
            else:
                handle_error(f"Unknown task: {task_name}")
                return {"status": "error", "message": "Unknown task."}

            logger.info(f"Task {task_id} completed successfully with result: {result}")
            return {"status": "success", "task-id": task_id, "result": result}
        except Exception as e:
            handle_error(f"Error executing task {task_id}: {e}")
            return {"status": "error", "message": str(e)}

    def add(self, *args):
        """Add numbers."""
        return sum(args)

    def subtract(self, *args):
        """Subtract numbers."""
        if len(args) < 2:
            raise ValueError("Subtract requires at least two arguments.")
        return args[0] - sum(args[1:])

# Example usage of TaskHandler
if __name__ == "__main__":
    handler = TaskHandler()

    # Sample task for addition
    sample_task_add = {
        "task-id": "add-task-1",
        "task": "add",
        "args": [10, 5, 2]
    }

    # Sample task for subtraction
    sample_task_subtract = {
        "task-id": "subtract-task-1",
        "task": "subtract",
        "args": [10, 5, 2]
    }

    # Handling tasks
    result_add = handler.handle_task(sample_task_add)
    print(f"Result of addition task: {result_add}")

    result_subtract = handler.handle_task(sample_task_subtract)
    print(f"Result of subtraction task: {result_subtract}")
