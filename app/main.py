from fastapi import FastAPI, BackgroundTasks, Form, UploadFile, File, HTTPException
import csv, io, uuid
from fastapi.responses import StreamingResponse
from app.tasks import process_csv
from app.config import redis_client, s3_client, AWS_BUCKET_NAME
import re

MAX_ROWS = 1000  # Maximum number of rows allowed in CSV
MAX_IMAGES_PER_ROW = 10  # Maximum number of image URLs allowed per row
ALLOWED_IMAGE_FORMATS = {".jpg", ".jpeg", ".png"}  # Allowed image formats

def validate_csv(file_content: str):
    """Validates CSV content before processing. Raises HTTPException on failure."""
    csv_data = list(csv.reader(file_content.splitlines()))
    
    # Check if file is empty
    if not csv_data or len(csv_data) < 2:
        raise HTTPException(status_code=400, detail="CSV file is empty or missing data.")

    # Validate header
    expected_headers = ["sr no.", "product name"]
    print(csv_data[0][:2])
    if not all(header.lower() in csv_data[0][:2] for header in expected_headers):
        raise HTTPException(status_code=400, detail="Invalid CSV headers. Expected: 'sr no.', 'product name', and 'image urls'.")

    total_rows = len(csv_data) - 1  # Exclude header row
    if total_rows > MAX_ROWS:
        raise HTTPException(status_code=400, detail=f"CSV exceeds maximum allowed rows ({MAX_ROWS}).")

    sr_no_set = set()
    
    for row_number, row in enumerate(csv_data[1:], start=1):  # Skip header
        if len(row) < 3:
            raise HTTPException(status_code=400, detail=f"Row {row_number}: Missing image URLs.")

        sr_no = row[0].strip()
        product_name = row[1].strip()
        image_urls = [url.strip() for url in row[2:] if url]

        # Validate Serial Number
        if not re.match(r"^[a-zA-Z0-9_-]+$", sr_no):
            raise HTTPException(status_code=400, detail=f"Row {row_number}: Invalid serial number format.")

        if sr_no in sr_no_set:
            raise HTTPException(status_code=400, detail=f"Row {row_number}: Duplicate serial number '{sr_no}' found.")
        sr_no_set.add(sr_no)

        # Validate Product Name
        if not re.match(r"^[a-zA-Z0-9\s_-]+$", product_name):
            raise HTTPException(status_code=400, detail=f"Row {row_number}: Invalid product name format.")

        # Validate Image URLs
        if not image_urls:
            raise HTTPException(status_code=400, detail=f"Row {row_number}: At least one image URL is required.")

        if len(image_urls) > MAX_IMAGES_PER_ROW:
            raise HTTPException(status_code=400, detail=f"Row {row_number}: Exceeds max {MAX_IMAGES_PER_ROW} images per row.")

        for url in image_urls:
            if not re.match(r"^https?://.*\.(jpg|jpeg|png)$", url, re.IGNORECASE):
                raise HTTPException(status_code=400, detail=f"Row {row_number}: Invalid image URL format '{url}'.")

    return True

app = FastAPI()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...), webhook_url: str = Form(None)):
    content = await file.read() 
    try:
        validate_csv(content.decode("utf-8"))
    except HTTPException as e:
        return {"error": e.detail} 

    print("outside validate_csv")
    request_id = str(uuid.uuid4()) #create a unique id for the request
    redis_client.set(f"request:{request_id}:status", "processing") #store status are processing for the id in redis
    if webhook_url:
        redis_client.set(f"request:{request_id}:webhook_url", webhook_url)#store webhook url for the id in redis

    file.file.seek(0)

    # Upload CSV to S3
    s3_filename = f"{request_id}/csv_uploads/{request_id}.csv"
    s3_client.upload_fileobj(file.file, AWS_BUCKET_NAME, s3_filename,  ExtraArgs={'ContentType': 'text/csv'})#upload inupt file to S3

    # Store the S3 file URL in Redis instead of full CSV data
    file_url = f"https://{AWS_BUCKET_NAME}.s3.amazonaws.com/{s3_filename}"
    redis_client.set(f"request:{request_id}:csv_url", file_url)#store input file url's S3 location in redis

    process_csv.delay(request_id)#asyncly process the file

    return {"request_id": request_id, "status": "processing"}

@app.get("/status/{request_id}")
def get_status(request_id: str):
    """Check processing status of a CSV file."""
    status = redis_client.get(f"request:{request_id}:status") or "unknown"
    processed_rows = int(redis_client.get(f"request:{request_id}:processed_rows") or 0)
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 1)  # Avoid division by zero
    csv_url = redis_client.get(f"request:{request_id}:csv_url")

    return {
        "request_id": request_id,
        "status": status,
        "processed_rows": processed_rows,
        "total_rows": total_rows,
        "progress": f"{(processed_rows / total_rows) * 100:.2f}%",
        "csv_url": csv_url if status == "csv_ready" else None
    }

#TODO: change to older version where streamingresponse was sent
@app.get("/download/{request_id}")
def download_csv(request_id: str):
    csv_url = redis_client.get(f"request:{request_id}:csv_url")

    if not csv_url:
        return {"error": "CSV file not found or processing not completed yet."}

    return {"request_id": request_id, "csv_url": csv_url}
   