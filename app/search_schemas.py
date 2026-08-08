from typing import List

from pydantic import BaseModel


class ImageSearchResult(BaseModel):
    point_id: str
    score: float
    request_id: str
    row_number: int
    image_index: int
    product_name: str
    original_url: str
    compressed_url: str


class ImageSearchResponse(BaseModel):
    query: str
    results: List[ImageSearchResult]
