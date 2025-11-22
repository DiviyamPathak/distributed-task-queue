from celery import Celery
import os

BROKER = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672//")
BACKEND = os.getenv("RESULT_BACKEND", "redis://redis:6379/0")

app = Celery("mtasks", broker=BROKER, backend=BACKEND)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_time_limit=300,
    task_soft_time_limit=240,
)
