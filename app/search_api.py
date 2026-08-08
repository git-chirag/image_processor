import logging
from io import BytesIO
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from PIL import Image, UnidentifiedImageError

from app.search_schemas import ImageSearchResponse, VisualSearchResponse
from app.search_service import ImageSearchService, get_image_search_service


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/images", tags=["image-search"])
MAX_SEARCH_IMAGE_BYTES = 10 * 1024 * 1024
MAX_SEARCH_IMAGE_PIXELS = 25_000_000
ALLOWED_IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP"}


@router.get("/search", response_model=ImageSearchResponse)
def search_images(
    q: str = Query(..., min_length=1, max_length=500),
    limit: int = Query(10, ge=1, le=50),
    score_threshold: Optional[float] = Query(None, ge=-1.0, le=1.0),
    service: ImageSearchService = Depends(get_image_search_service),
):
    query = q.strip()
    if not query:
        raise HTTPException(status_code=422, detail="Search query must not be empty.")

    try:
        results = service.search(
            query=query,
            limit=limit,
            score_threshold=score_threshold,
        )
    except Exception as exc:
        logger.exception("Image search failed query=%r", query)
        raise HTTPException(
            status_code=503,
            detail="Image search is temporarily unavailable.",
        ) from exc

    return ImageSearchResponse(query=query, results=results)


@router.post("/search-by-image", response_model=VisualSearchResponse)
def search_by_image(
    file: UploadFile = File(...),
    limit: int = Query(10, ge=1, le=50),
    score_threshold: Optional[float] = Query(None, ge=-1.0, le=1.0),
    service: ImageSearchService = Depends(get_image_search_service),
):
    if file.content_type not in ALLOWED_IMAGE_CONTENT_TYPES:
        raise HTTPException(
            status_code=415,
            detail="Only JPEG, PNG, and WebP images are supported.",
        )

    image_content = file.file.read(MAX_SEARCH_IMAGE_BYTES + 1)
    if not image_content:
        raise HTTPException(status_code=400, detail="Uploaded image is empty.")
    if len(image_content) > MAX_SEARCH_IMAGE_BYTES:
        raise HTTPException(
            status_code=413,
            detail="Uploaded image must not exceed 10 MB.",
        )

    _validate_search_image(image_content)

    try:
        results = service.search_by_image(
            image_content=image_content,
            limit=limit,
            score_threshold=score_threshold,
        )
    except Exception as exc:
        logger.exception("Visual image search failed filename=%r", file.filename)
        raise HTTPException(
            status_code=503,
            detail="Image search is temporarily unavailable.",
        ) from exc

    return VisualSearchResponse(
        filename=file.filename or "uploaded-image",
        results=results,
    )


def _validate_search_image(image_content):
    try:
        with Image.open(BytesIO(image_content)) as image:
            if image.format not in ALLOWED_IMAGE_FORMATS:
                raise HTTPException(
                    status_code=415,
                    detail="Only JPEG, PNG, and WebP images are supported.",
                )
            if image.width * image.height > MAX_SEARCH_IMAGE_PIXELS:
                raise HTTPException(
                    status_code=413,
                    detail="Uploaded image dimensions are too large.",
                )
            image.verify()
    except Image.DecompressionBombError as exc:
        raise HTTPException(
            status_code=413,
            detail="Uploaded image dimensions are too large.",
        ) from exc
    except (UnidentifiedImageError, OSError) as exc:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file is not a valid image.",
        ) from exc
