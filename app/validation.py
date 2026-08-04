import csv
import io
import re
from urllib.parse import urlparse

from fastapi import HTTPException


MAX_ROWS = 1000
MAX_IMAGES_PER_ROW = 10
EXPECTED_HEADERS = ["Sr. No", "Product Name", "Input Image URLs"]
SUPPORTED_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")


def validate_csv(file_content: str) -> None:
    """Validate CSV content before creating a processing request."""
    csv_data = list(csv.reader(io.StringIO(file_content)))

    if len(csv_data) < 2:
        raise HTTPException(
            status_code=400,
            detail="CSV file is empty or missing data.",
        )

    # Header order matters because workers read columns by index.
    csv_headers = [header.strip() for header in csv_data[0][:3]]
    if csv_headers != EXPECTED_HEADERS:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid CSV headers. Expected: 'Sr. No', "
                "'Product Name', and 'Input Image URLs'."
            ),
        )

    total_rows = len(csv_data) - 1
    if total_rows > MAX_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"CSV exceeds maximum allowed rows ({MAX_ROWS}).",
        )

    serial_numbers = set()
    for row_number, row in enumerate(csv_data[1:], start=1):
        if len(row) < 3:
            raise HTTPException(
                status_code=400,
                detail=f"Row {row_number}: Missing image URLs.",
            )

        serial_number = row[0].strip()
        product_name = row[1].strip()
        image_urls = [url.strip() for url in row[2:] if url.strip()]

        if not re.fullmatch(r"[a-zA-Z0-9_-]+", serial_number):
            raise HTTPException(
                status_code=400,
                detail=f"Row {row_number}: Invalid serial number format.",
            )

        if serial_number in serial_numbers:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Row {row_number}: Duplicate serial number "
                    f"'{serial_number}' found."
                ),
            )
        serial_numbers.add(serial_number)

        if not re.fullmatch(r"[a-zA-Z0-9\s_-]+", product_name):
            raise HTTPException(
                status_code=400,
                detail=f"Row {row_number}: Invalid product name format.",
            )

        if not image_urls:
            raise HTTPException(
                status_code=400,
                detail=f"Row {row_number}: At least one image URL is required.",
            )

        if len(image_urls) > MAX_IMAGES_PER_ROW:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Row {row_number}: Exceeds max "
                    f"{MAX_IMAGES_PER_ROW} images per row."
                ),
            )

        for image_url in image_urls:
            parsed_url = urlparse(image_url)
            is_supported_image = parsed_url.path.lower().endswith(
                SUPPORTED_IMAGE_EXTENSIONS
            )
            if (
                parsed_url.scheme not in {"http", "https"}
                or not parsed_url.netloc
                or not is_supported_image
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Row {row_number}: Invalid image URL format "
                        f"'{image_url}'."
                    ),
                )

