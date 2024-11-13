from celery import Celery
from core.config.development import KAFKA_BROKER_URL, RESULT_BACKEND_URL

# Create a Celery application instance
app = Celery('yadtq')

# Configure Celery to use Kafka as the broker and Redis (or another backend) for results
app.conf.update(
    broker_url=KAFKA_BROKER_URL,
    result_backend=RESULT_BACKEND_URL,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
)

@app.task(bind=True)
def add(self, x, y):
    """Example task that adds two numbers."""
    return x + y

@app.task(bind=True)
def subtract(self, x, y):
    """Example task that subtracts two numbers."""
    return x - y

@app.task(bind=True)
def multiply(self, x, y):
    """Example task that multiplies two numbers."""
    return x * y

@app.task(bind=True)
def divide(self, x, y):
    """Example task that divides two numbers."""
    if y == 0:
        raise ValueError("Cannot divide by zero.")
    return x / y

if __name__ == "__main__":
    app.start()