import io
import botocore.exceptions
from celery import chain, chord, group
import requests
from app.celery_worker import celery
from app.config import s3_client, AWS_BUCKET_NAME, AWS_REGION, redis_client, CLOUDINARY_FETCH_URL
import csv
import time


#TODO: find an alterantive for headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}


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


@celery.task(name="process_csv")
def process_csv(request_id):
    """Process the CSV asynchronously in a Celery task."""
    try:
        csv_url = redis_client.get(f"request:{request_id}:csv_url") #fetch input file's url from redis
        if not csv_url:
            redis_client.set(f"request:{request_id}:status", "failed")
            return {"error": "CSV file URL not found"}
        
        response = requests.get(csv_url)#retrieve input file from S3
        if response.status_code != 200:
            redis_client.set(f"request:{request_id}:status", "failed")
            return {"error": "Failed to download CSV from S3"}

        csv_data = list(csv.reader(io.StringIO(response.text)))
        data_rows = csv_data[1:]  # Skip header

        total_rows = len(data_rows)
        redis_client.set(f"request:{request_id}:total_rows", total_rows)
        redis_client.set(f"request:{request_id}:processed_rows", 0)
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
            redis_client.set(f"request:{request_id}:status", "failed")
            return {"error": "No image tasks were created"}

        # Fan out image work and finalize only after every task succeeds.
        chord(group(image_tasks))(
            on_all_images_complete.s(request_id)
        )

    except Exception as e:
        redis_client.set(f"request:{request_id}:status", "failed")
        print(f"Error processing CSV {request_id}: {e}")

def get_compressed_url(image_url):
    """Generate Cloudinary fetch URL for compression."""
    return f"{CLOUDINARY_FETCH_URL}/q_50,f_auto/{image_url}?_={int(time.time())}"

@celery.task(name="generate_output_csv")
def generate_output_csv(request_id):
    """Generate the output CSV and store it in S3."""
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    if total_rows == 0:
        return {"error": "Invalid request ID or no processed data"}

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
    try:
        s3_client.put_object(Bucket=AWS_BUCKET_NAME, Key=s3_filename, Body=output.getvalue(), ContentType="text/csv")

        # Store the S3 file URL in Redis
        csv_url = f"https://{AWS_BUCKET_NAME}.s3.amazonaws.com/{s3_filename}"
        redis_client.set(f"request:{request_id}:csv_url", csv_url)

        # Update status to csv_ready **only after successful upload**
        redis_client.set(f"request:{request_id}:status", "csv_ready")

        return request_id

    except Exception as e:
        print(f"Error uploading CSV to S3: {e}")
        redis_client.set(f"request:{request_id}:status", "csv_upload_failed")  # New status for failed upload
        return {"error": "Failed to upload CSV"}


@celery.task(name="on_all_images_complete")
def on_all_images_complete(results, request_id):
    """Start finalization after the chord confirms all image tasks succeeded."""
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    redis_client.set(f"request:{request_id}:status", "completed")

    # Preserve finalization order while keeping each step as a Celery task.
    chain(
        generate_output_csv.s(request_id),
        send_webhook_notification.s(total_rows),
    ).apply_async()

    return {
        "request_id": request_id,
        "processed_images": len(results),
    }
    
@celery.task(name="send_webhook_notification")
def send_webhook_notification(request_id, total_rows):
    """ Send webhook notification after CSV generation is complete """
    webhook_url = redis_client.get(f"request:{request_id}:webhook_url")
    if webhook_url:
        payload = {
            "request_id": request_id,
            "status": "csv_ready",
            "total_rows": total_rows,
            "message": "CSV processing completed successfully.",
        }
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Webhook request failed for {webhook_url}: {e}")

@celery.task(name="compress_image", autoretry_for=(requests.exceptions.RequestException, IOError, botocore.exceptions.ClientError),
             retry_kwargs={"max_retries": 3, "countdown": 5}, retry_backoff=True)
def compress_image(request_id, row_number, image_url, image_index):
    """Download, compress, and return processed image data."""
    try:
        cloudinary_compressed_url = get_compressed_url(image_url)


        # Download the compressed image from Cloudinary
        response = requests.get(cloudinary_compressed_url, stream=True, timeout=10)
        response.raise_for_status()

        # Convert response to file-like object for S3 upload
        img_bytes = io.BytesIO(response.content)


        # Upload compressed image to S3
        s3_filename = f"{request_id}/{row_number}_{image_index}.jpg"
        s3_client.upload_fileobj(img_bytes, AWS_BUCKET_NAME, s3_filename, ExtraArgs={'ContentType': 'image/jpeg'})
        compressed_url = f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{s3_filename}"

    except (requests.exceptions.RequestException, botocore.exceptions.ClientError, ValueError) as e:
        print(f"Error processing image {image_url}: {e}")
        compress_image.retry(exc=e, countdown=5)

    

    redis_client.hset(f"request:{request_id}:row:{row_number}", f"processed_image_{image_index}", compressed_url)
    mark_image_complete(request_id, row_number, image_index)
    return {"compressed_url": compressed_url}
