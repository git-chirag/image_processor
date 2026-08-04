from fastapi import FastAPI, Form, UploadFile, File, HTTPException
import uuid
from fastapi.responses import StreamingResponse
from app.tasks import process_csv
from app.config import redis_client, s3_client, AWS_BUCKET_NAME
from app.validation import validate_csv
import requests

app = FastAPI()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...), webhook_url: str = Form(None)):
    content = await file.read()
    try:
        decoded_content = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail="CSV must be UTF-8 encoded.",
        ) from exc

    # Validation errors propagate so FastAPI returns the intended HTTP 400.
    validate_csv(decoded_content)

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

@app.get("/download/{request_id}")
def download_csv(request_id: str):
    """Fetch the CSV file from S3 and return it as a streaming response."""
    
    csv_url = redis_client.get(f"request:{request_id}:csv_url")

    if not csv_url:
        return {"error": "CSV file not found or processing not completed yet."}

    # Fetch the CSV file from S3
    response = requests.get(csv_url, stream=True)
    
    if response.status_code != 200:
        return {"error": "Failed to fetch CSV file from S3."}

    return StreamingResponse(response.iter_content(chunk_size=1024), 
                             media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={request_id}.csv"})
