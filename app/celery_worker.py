from celery import Celery
from app.config import REDIS_URL

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL, include=["app.tasks"])

celery.conf.update(task_track_started=True)

# Ensure Celery does not crash on Redis disconnection
celery.conf.broker_transport_options = {
    "max_retries": 5,  # Retry 5 times before failing
    "interval_start": 0.1,  # Wait at least 0.1s before retrying
    "interval_step": 0.2,  # Increase wait time by 0.2s per retry
    "interval_max": 1.0,  # Max wait time of 1s between retries
}