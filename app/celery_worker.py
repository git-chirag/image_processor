from celery import Celery
from kombu import Queue

from app.config import REDIS_URL, REQUEST_TTL_SECONDS

celery = Celery("tasks", broker=REDIS_URL, backend=REDIS_URL, include=["app.tasks"])

celery.conf.update(
    task_track_started=True,
    result_expires=REQUEST_TTL_SECONDS,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    task_default_queue="orchestration",
    task_queues=(
        Queue("orchestration"),
        Queue("image_processing"),
        Queue("csv_generation"),
        Queue("webhooks"),
    ),
    task_routes={
        "process_csv": {"queue": "orchestration"},
        "on_all_images_complete": {"queue": "orchestration"},
        "compress_image": {"queue": "image_processing"},
        "generate_output_csv": {"queue": "csv_generation"},
        "send_webhook_notification": {"queue": "webhooks"},
    },
    task_create_missing_queues=False,
)

# Ensure Celery does not crash on Redis disconnection
celery.conf.broker_transport_options = {
    "max_retries": 5,  # Retry 5 times before failing
    "interval_start": 0.1,  # Wait at least 0.1s before retrying
    "interval_step": 0.2,  # Increase wait time by 0.2s per retry
    "interval_max": 1.0,  # Max wait time of 1s between retries
}
