import unittest

from app.image_indexer import index_processed_image
from app.qdrant_config import CLIP_VECTOR_SIZE
from app.vector_store import build_image_point_id


class FakeEncoder:
    def __init__(self):
        self.image_content = None

    def encode_image(self, image_content):
        self.image_content = image_content
        return [1.0] + [0.0] * (CLIP_VECTOR_SIZE - 1)


class FakeVectorStore:
    def __init__(self):
        self.upserts = []

    def upsert(self, point_id, vector, payload):
        self.upserts.append(
            {
                "point_id": point_id,
                "vector": vector,
                "payload": payload,
            }
        )


class ImageIndexerTests(unittest.TestCase):
    def setUp(self):
        self.encoder = FakeEncoder()
        self.vector_store = FakeVectorStore()

    def test_processed_image_is_encoded_and_upserted_with_metadata(self):
        point_id = index_processed_image(
            request_id="request-1",
            row_number=2,
            image_index=3,
            image_url="https://source.example/image.jpg",
            compressed_url="https://bucket.example/processed.jpg",
            image_content=b"image bytes",
            product_name="Sample product",
            encoder=self.encoder,
            vector_store=self.vector_store,
        )

        self.assertEqual(self.encoder.image_content, b"image bytes")
        self.assertEqual(
            point_id,
            build_image_point_id("request-1", 2, 3),
        )
        self.assertEqual(len(self.vector_store.upserts), 1)
        self.assertEqual(
            self.vector_store.upserts[0]["payload"],
            {
                "request_id": "request-1",
                "row_number": 2,
                "image_index": 3,
                "product_name": "Sample product",
                "original_url": "https://source.example/image.jpg",
                "compressed_url": "https://bucket.example/processed.jpg",
            },
        )
        self.assertEqual(
            len(self.vector_store.upserts[0]["vector"]),
            CLIP_VECTOR_SIZE,
        )

    def test_retry_uses_the_same_point_id(self):
        arguments = {
            "request_id": "request-1",
            "row_number": 2,
            "image_index": 3,
            "image_url": "https://source.example/image.jpg",
            "compressed_url": "https://bucket.example/processed.jpg",
            "image_content": b"image bytes",
            "product_name": "Sample product",
            "encoder": self.encoder,
            "vector_store": self.vector_store,
        }

        first_id = index_processed_image(**arguments)
        retried_id = index_processed_image(**arguments)

        self.assertEqual(first_id, retried_id)
        self.assertEqual(len(self.vector_store.upserts), 2)


if __name__ == "__main__":
    unittest.main()
