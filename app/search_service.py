from functools import lru_cache

from app.clip_encoder import get_clip_encoder
from app.vector_store import get_image_vector_store


class ImageSearchService:
    def __init__(self, encoder=None, vector_store=None):
        self.encoder = encoder
        self.vector_store = vector_store

    def search(self, query, limit=10, score_threshold=None):
        normalized_query = query.strip()
        if not normalized_query:
            raise ValueError("Search query must not be empty.")

        encoder = self.encoder or get_clip_encoder()
        vector_store = self.vector_store or get_image_vector_store()
        query_vector = encoder.encode_text(normalized_query)
        return self._search_vector_store(
            vector_store=vector_store,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        )

    def search_by_image(self, image_content, limit=10, score_threshold=None):
        encoder = self.encoder or get_clip_encoder()
        vector_store = self.vector_store or get_image_vector_store()
        query_vector = encoder.encode_image(image_content)
        return self._search_vector_store(
            vector_store=vector_store,
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        )

    def _search_vector_store(
        self,
        vector_store,
        query_vector,
        limit,
        score_threshold,
    ):
        points = vector_store.search(
            query_vector=query_vector,
            limit=limit,
            score_threshold=score_threshold,
        )
        return [self._format_result(point) for point in points]

    @staticmethod
    def _format_result(point):
        payload = point.payload or {}
        return {
            "point_id": str(point.id),
            "score": float(point.score),
            "request_id": payload["request_id"],
            "row_number": payload["row_number"],
            "image_index": payload["image_index"],
            "product_name": payload["product_name"],
            "original_url": payload["original_url"],
            "compressed_url": payload["compressed_url"],
        }


@lru_cache(maxsize=1)
def get_image_search_service():
    return ImageSearchService()
