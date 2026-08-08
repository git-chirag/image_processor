import logging
import uuid

import botocore.exceptions
from fastapi import FastAPI, Form, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse

from app.tasks import process_csv
from app.config import redis_client, s3_client, AWS_BUCKET_NAME
from app.redis_state import set_request_value
from app.search_api import router as image_search_router
from app.validation import validate_csv


logger = logging.getLogger(__name__)
app = FastAPI()
app.include_router(image_search_router)

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

    request_id = str(uuid.uuid4())
    file.file.seek(0)

    input_csv_key = f"{request_id}/csv_uploads/{request_id}.csv"
    try:
        s3_client.upload_fileobj(
            file.file,
            AWS_BUCKET_NAME,
            input_csv_key,
            ExtraArgs={"ContentType": "text/csv"},
        )
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as exc:
        raise HTTPException(
            status_code=503,
            detail="Unable to store the CSV for processing.",
        ) from exc

    set_request_value(request_id, "status", "processing")
    set_request_value(request_id, "input_csv_key", input_csv_key)
    if webhook_url:
        set_request_value(request_id, "webhook_url", webhook_url)
        set_request_value(request_id, "webhook_status", "pending")

    process_csv.delay(request_id)
    logger.info("Accepted CSV request_id=%s", request_id)

    return {"request_id": request_id, "status": "processing"}

@app.get("/status/{request_id}")
def get_status(request_id: str):
    """Check processing status of a CSV file."""
    status = redis_client.get(f"request:{request_id}:status")
    if status is None:
        raise HTTPException(status_code=404, detail="Request not found.")

    processed_rows = int(redis_client.get(f"request:{request_id}:processed_rows") or 0)
    total_rows = int(redis_client.get(f"request:{request_id}:total_rows") or 0)
    progress = (processed_rows / total_rows) * 100 if total_rows else 0

    csv_url = None
    output_csv_key = redis_client.get(f"request:{request_id}:output_csv_key")
    if status == "csv_ready" and output_csv_key:
        csv_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": AWS_BUCKET_NAME, "Key": output_csv_key},
            ExpiresIn=900,
        )

    return {
        "request_id": request_id,
        "status": status,
        "processed_rows": processed_rows,
        "total_rows": total_rows,
        "progress": f"{progress:.2f}%",
        "csv_url": csv_url,
    }

@app.get("/download/{request_id}")
def download_csv(request_id: str):
    """Fetch the completed output CSV from S3."""
    status = redis_client.get(f"request:{request_id}:status")
    if status is None:
        raise HTTPException(status_code=404, detail="Request not found.")
    if status != "csv_ready":
        raise HTTPException(
            status_code=409,
            detail="CSV processing is not complete.",
        )

    output_csv_key = redis_client.get(f"request:{request_id}:output_csv_key")
    if not output_csv_key:
        raise HTTPException(status_code=404, detail="Output CSV not found.")

    try:
        response = s3_client.get_object(
            Bucket=AWS_BUCKET_NAME,
            Key=output_csv_key,
        )
    except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as exc:
        raise HTTPException(
            status_code=502,
            detail="Failed to fetch CSV file from S3.",
        ) from exc

    return StreamingResponse(
        response["Body"].iter_chunks(chunk_size=1024),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={request_id}.csv"},
    )
