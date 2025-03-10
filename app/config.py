import os
from dotenv import load_dotenv
import boto3
import redis
import time
import cloudinary
import cloudinary.uploader

def get_redis_client(retries=5, delay=2):
    for attempt in range(retries):
        try:
            client = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
            client.ping()  # Ensure connection is alive
            return client
        except redis.exceptions.ConnectionError as e:
            print(f"Redis connection failed (Attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise redis.exceptions.ConnectionError("Failed to connect to Redis after multiple attempts")



load_dotenv()  # Load environment variables

# Redis Configuration
REDIS_URL = os.getenv(
    "REDIS_URL"
)
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
CLOUDINARY_FETCH_URL = os.getenv("CLOUDINARY_FETCH_URL")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

redis_client = get_redis_client()
