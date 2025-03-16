import io
import botocore.exceptions
from celery import chain
import requests
from app.celery_worker import celery
from app.config import s3_client, AWS_BUCKET_NAME, AWS_REGION, redis_client, CLOUDINARY_FETCH_URL
import csv
import time


#TODO: find an alterantive for headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}


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

            #redis_client.rpush(f"request:{request_id}:row:{row_number}:image_urls", *image_urls)#store input-urls as list for each row

            for index, url in enumerate(image_urls):
                redis_client.hset(f"request:{request_id}:row:{row_number}", f"image_{index}", url)
                compress_image.delay(request_id, row_number, url, index)  # Process images asynchronously

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

@celery.task(name="check_csv_completion", max_retries=5, default_retry_delay=10)
def check_csv_completion(request_id, row_number):
    """Check if all images and rows are processed, then trigger CSV generation and webhook notification."""
    
    # Get total images in the row
    total_images = int(redis_client.hget(f"request:{request_id}:row:{row_number}", "image_count") or 0)
    
    # Count how many images have been processed
    processed_images = int(redis_client.hget(f"request:{request_id}:row:{row_number}", "processed_image_count") or 0)

    if processed_images == total_images:
        row_processed_key = f"request:{request_id}:row:{row_number}:processed"
        if not redis_client.exists(row_processed_key):
            redis_client.incr(f"request:{request_id}:processed_rows")
            redis_client.set(row_processed_key, "true")  # Mark this row as processed
    
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    processed_rows = int(redis_client.get(f"request:{request_id}:processed_rows") or 0)

    if processed_rows == total_rows:
        redis_client.set(f"request:{request_id}:status", "completed")  # Mark CSV as completed

        try:
            chain(generate_output_csv.s(request_id), send_webhook_notification.s(total_rows)).apply_async()
        except check_csv_completion.max_retries:  
            redis_client.set(f"request:{request_id}:status", "failed")
            print(f"CSV processing failed after max retries for request {request_id}")


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

    

    # Store the processed image URL in Redis
    # redis_client.rpush(f"request:{request_id}:row:{row_number}:processed_urls", compressed_url)
    redis_client.hset(f"request:{request_id}:row:{row_number}", f"processed_image_{image_index}", compressed_url)
    redis_client.hincrby(f"request:{request_id}:row:{row_number}", "processed_image_count", 1)

    check_csv_completion.apply_async((request_id, row_number))
    return {"compressed_url": compressed_url}