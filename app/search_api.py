import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.search_schemas import ImageSearchResponse
from app.search_service import ImageSearchService, get_image_search_service


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/images", tags=["image-search"])


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
