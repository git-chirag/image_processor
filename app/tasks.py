import io
import botocore.exceptions
from celery import Task, chain, chord, group
import requests
from app.celery_worker import celery
from app.config import s3_client, AWS_BUCKET_NAME, AWS_REGION, redis_client, CLOUDINARY_FETCH_URL
import csv


#TODO: find an alterantive for headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}


MARK_REQUEST_FAILED_SCRIPT = redis_client.register_script("""
    local status = redis.call("GET", KEYS[1])
    if status ~= "csv_ready" then
        redis.call("SET", KEYS[1], "failed")
        return 1
    end
    return 0
""")


class RequestTask(Task):
    """Record terminal processing failures against the owning request."""

    abstract = True
    request_id_arg_index = 0

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if len(args) > self.request_id_arg_index:
            request_id = args[self.request_id_arg_index]
            try:
                MARK_REQUEST_FAILED_SCRIPT(
                    keys=[f"request:{request_id}:status"]
                )
            except Exception as status_error:
                print(
                    f"Could not record failure for task {task_id}: "
                    f"{status_error}"
                )
        super().on_failure(exc, task_id, args, kwargs, einfo)


class ChordCallbackTask(RequestTask):
    """Read request_id after the chord's results argument."""

    abstract = True
    request_id_arg_index = 1


class WebhookTask(Task):
    """Track webhook failure without failing a completed CSV request."""

    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if args:
            request_id = args[0]
            try:
                redis_client.set(
                    f"request:{request_id}:webhook_status",
                    "failed",
                )
            except Exception as status_error:
                print(
                    f"Could not record webhook failure for task {task_id}: "
                    f"{status_error}"
                )
        super().on_failure(exc, task_id, args, kwargs, einfo)


# Atomically count each completed image and row once, including after retries.
MARK_IMAGE_COMPLETE_SCRIPT = redis_client.register_script("""
    local image_was_new = redis.call("SETNX", KEYS[1], "true")
    if image_was_new == 0 then
        return tonumber(redis.call("HGET", KEYS[4], "processed_image_count") or "0")
    end

    local processed_images = redis.call("HINCRBY", KEYS[4], "processed_image_count", 1)
    local total_images = tonumber(redis.call("HGET", KEYS[4], "image_count") or "0")

    if processed_images == total_images then
        local row_was_new = redis.call("SETNX", KEYS[2], "true")
        if row_was_new == 1 then
            redis.call("INCR", KEYS[3])
        end
    end

    return processed_images
""")


def mark_image_complete(request_id, row_number, image_index):
    """Atomically update image, row, and request progress in Redis."""
    row_key = f"request:{request_id}:row:{row_number}"
    MARK_IMAGE_COMPLETE_SCRIPT(
        keys=[
            f"{row_key}:image:{image_index}:processed",
            f"{row_key}:processed",
            f"request:{request_id}:processed_rows",
            row_key,
        ]
    )


@celery.task(
    base=RequestTask,
    name="process_csv",
    autoretry_for=(
        botocore.exceptions.BotoCoreError,
        botocore.exceptions.ClientError,
    ),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def process_csv(request_id):
    """Process the CSV asynchronously in a Celery task."""
    input_csv_key = redis_client.get(f"request:{request_id}:input_csv_key")
    if not input_csv_key:
        raise ValueError("Input CSV key not found")

    response = s3_client.get_object(
        Bucket=AWS_BUCKET_NAME,
        Key=input_csv_key,
    )
    csv_content = response["Body"].read().decode("utf-8")

    csv_data = list(csv.reader(io.StringIO(csv_content)))
    data_rows = csv_data[1:]

    total_rows = len(data_rows)
    redis_client.set(f"request:{request_id}:total_rows", total_rows)
    redis_client.set(
        f"request:{request_id}:processed_rows",
        0,
        nx=True,
    )
    image_tasks = []

    for row_number, row in enumerate(data_rows, start=1):
        sr_no = row[0].strip()
        product_name = row[1].strip()
        image_urls = [url.strip() for url in row[2:] if url]
        image_count = len(image_urls)

        redis_client.hset(f"request:{request_id}:row:{row_number}", mapping={
            "sr_no": sr_no,
            "product_name": product_name,
            "image_count": image_count
        })

        for index, url in enumerate(image_urls):
            redis_client.hset(f"request:{request_id}:row:{row_number}", f"image_{index}", url)
            image_tasks.append(
                compress_image.s(request_id, row_number, url, index)
            )

    if not image_tasks:
        raise ValueError("No image tasks were created")

    # Fan out image work and finalize only after every task succeeds.
    chord(group(image_tasks))(
        on_all_images_complete.s(request_id)
    )

def get_compressed_url(image_url):
    """Generate Cloudinary fetch URL for compression."""
    return f"{CLOUDINARY_FETCH_URL}/q_50,f_jpg/{image_url}"

@celery.task(
    base=RequestTask,
    name="generate_output_csv",
    autoretry_for=(
        botocore.exceptions.BotoCoreError,
        botocore.exceptions.ClientError,
    ),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 3},
)
def generate_output_csv(request_id):
    """Generate the output CSV and store it in S3."""
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    if total_rows == 0:
        raise ValueError("Invalid request ID or no processed data")

    output = io.StringIO()
    csv_writer = csv.writer(output)
    
    # Write header
    csv_writer.writerow(["sr no.", "product name", "original images", "compressed images"])

    for row_number in range(1, total_rows + 1):
        sr_no = redis_client.hget(f"request:{request_id}:row:{row_number}", "sr_no") or ""
        product_name = redis_client.hget(f"request:{request_id}:row:{row_number}", "product_name") or ""
        image_count = int(redis_client.hget(f"request:{request_id}:row:{row_number}", "image_count") or 0)

        original_urls = []
        compressed_urls = []

        for index in range(image_count):  # Assuming max 10 images per row
            original_url = redis_client.hget(f"request:{request_id}:row:{row_number}", f"image_{index}")
            compressed_url = redis_client.hget(f"request:{request_id}:row:{row_number}", f"processed_image_{index}")
            
            if original_url:
                original_urls.append(original_url)
            if compressed_url:
                compressed_urls.append(compressed_url)

        csv_writer.writerow([sr_no, product_name, ",".join(original_urls), ",".join(compressed_urls)])
        
    # Upload CSV to S3
    output.seek(0)
    s3_filename = f"processed_csv/{request_id}.csv"
    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=s3_filename,
        Body=output.getvalue(),
        ContentType="text/csv",
    )

    redis_client.set(f"request:{request_id}:output_csv_key", s3_filename)
    redis_client.set(f"request:{request_id}:status", "csv_ready")
    return request_id


@celery.task(base=ChordCallbackTask, name="on_all_images_complete")
def on_all_images_complete(results, request_id):
    """Start finalization after the chord confirms all image tasks succeeded."""
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    # Preserve finalization order while keeping each step as a Celery task.
    chain(
        generate_output_csv.s(request_id),
        send_webhook_notification.s(total_rows),
    ).apply_async()

    return {
        "request_id": request_id,
        "processed_images": len(results),
    }
    
@celery.task(
    base=WebhookTask,
    name="send_webhook_notification",
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 5},
)
def send_webhook_notification(request_id, total_rows):
    """Send a webhook after CSV generation is complete."""
    webhook_url = redis_client.get(f"request:{request_id}:webhook_url")
    if not webhook_url:
        return {"status": "not_configured"}

    payload = {
        "request_id": request_id,
        "status": "csv_ready",
        "total_rows": total_rows,
        "message": "CSV processing completed successfully.",
    }
    response = requests.post(
        webhook_url,
        json=payload,
        headers={"Idempotency-Key": request_id},
        timeout=10,
    )
    response.raise_for_status()
    redis_client.set(f"request:{request_id}:webhook_status", "delivered")
    return {"status": "delivered"}

@celery.task(
    base=RequestTask,
    name="compress_image",
    autoretry_for=(
        requests.exceptions.RequestException,
        botocore.exceptions.BotoCoreError,
        botocore.exceptions.ClientError,
        OSError,
    ),
    retry_kwargs={"max_retries": 3},
    retry_backoff=True,
    retry_jitter=True,
)
def compress_image(request_id, row_number, image_url, image_index):
    """Download, compress, and return processed image data."""
    response = requests.get(
        get_compressed_url(image_url),
        stream=True,
        timeout=10,
    )
    response.raise_for_status()

    img_bytes = io.BytesIO(response.content)

    s3_filename = f"{request_id}/{row_number}_{image_index}.jpg"
    s3_client.upload_fileobj(
        img_bytes,
        AWS_BUCKET_NAME,
        s3_filename,
        ExtraArgs={"ContentType": "image/jpeg"},
    )
    compressed_url = (
        f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/"
        f"{s3_filename}"
    )

    redis_client.hset(f"request:{request_id}:row:{row_number}", f"processed_image_{image_index}", compressed_url)
    mark_image_complete(request_id, row_number, image_index)
    return {"compressed_url": compressed_url}
