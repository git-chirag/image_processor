import unittest
from types import SimpleNamespace

from app.qdrant_config import CLIP_VECTOR_SIZE
from app.search_service import ImageSearchService


class FakeEncoder:
    def __init__(self):
        self.query = None
        self.image_content = None

    def encode_text(self, query):
        self.query = query
        return [1.0] + [0.0] * (CLIP_VECTOR_SIZE - 1)

    def encode_image(self, image_content):
        self.image_content = image_content
        return [0.0, 1.0] + [0.0] * (CLIP_VECTOR_SIZE - 2)


class FakeVectorStore:
    def __init__(self, points):
        self.points = points
        self.search_arguments = None

    def search(self, **kwargs):
        self.search_arguments = kwargs
        return self.points


class ImageSearchServiceTests(unittest.TestCase):
    def test_text_is_encoded_and_qdrant_results_are_formatted(self):
        point = SimpleNamespace(
            id="point-1",
            score=0.875,
            payload={
                "request_id": "request-1",
                "row_number": 2,
                "image_index": 3,
                "product_name": "Dog poster",
                "original_url": "https://source.example/dog.jpg",
                "compressed_url": "https://bucket.example/dog.jpg",
            },
        )
        encoder = FakeEncoder()
        vector_store = FakeVectorStore([point])
        service = ImageSearchService(encoder, vector_store)

        results = service.search(
            "  golden retriever  ",
            limit=5,
            score_threshold=0.7,
        )

        self.assertEqual(encoder.query, "golden retriever")
        self.assertEqual(vector_store.search_arguments["limit"], 5)
        self.assertEqual(vector_store.search_arguments["score_threshold"], 0.7)
        self.assertEqual(
            len(vector_store.search_arguments["query_vector"]),
            CLIP_VECTOR_SIZE,
        )
        self.assertEqual(
            results,
            [{
                "point_id": "point-1",
                "score": 0.875,
                "request_id": "request-1",
                "row_number": 2,
                "image_index": 3,
                "product_name": "Dog poster",
                "original_url": "https://source.example/dog.jpg",
                "compressed_url": "https://bucket.example/dog.jpg",
            }],
        )

    def test_empty_query_is_rejected(self):
        service = ImageSearchService(FakeEncoder(), FakeVectorStore([]))

        with self.assertRaises(ValueError):
            service.search("   ")

    def test_no_matches_returns_an_empty_list(self):
        service = ImageSearchService(FakeEncoder(), FakeVectorStore([]))

        self.assertEqual(service.search("something absent"), [])

    def test_uploaded_image_is_encoded_and_used_for_vector_search(self):
        encoder = FakeEncoder()
        vector_store = FakeVectorStore([])
        service = ImageSearchService(encoder, vector_store)

        results = service.search_by_image(
            image_content=b"image bytes",
            limit=3,
            score_threshold=0.8,
        )

        self.assertEqual(results, [])
        self.assertEqual(encoder.image_content, b"image bytes")
        self.assertEqual(vector_store.search_arguments["limit"], 3)
        self.assertEqual(vector_store.search_arguments["score_threshold"], 0.8)
        self.assertEqual(
            len(vector_store.search_arguments["query_vector"]),
            CLIP_VECTOR_SIZE,
        )

    def test_near_duplicate_search_applies_strict_threshold(self):
        encoder = FakeEncoder()
        vector_store = FakeVectorStore([])
        service = ImageSearchService(encoder, vector_store)

        service.find_near_duplicates(
            image_content=b"image bytes",
            limit=4,
            threshold=0.97,
        )

        self.assertEqual(vector_store.search_arguments["limit"], 4)
        self.assertEqual(vector_store.search_arguments["score_threshold"], 0.97)


if __name__ == "__main__":
    unittest.main()
