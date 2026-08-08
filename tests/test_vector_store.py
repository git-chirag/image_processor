import unittest

from qdrant_client import QdrantClient, models

from app.qdrant_config import (
    CLIP_VECTOR_SIZE,
    QdrantConfigurationError,
    QdrantSettings,
    create_qdrant_client,
)
from app.vector_store import (
    CollectionConfigurationError,
    ImageVectorStore,
    InvalidVectorError,
    build_image_point_id,
)


class ImageVectorStoreTests(unittest.TestCase):
    def setUp(self):
        self.client = QdrantClient(":memory:")
        self.store = ImageVectorStore(
            client=self.client,
            collection_name="test_images",
            vector_size=CLIP_VECTOR_SIZE,
        )

    def test_collection_creation_is_idempotent(self):
        self.store.ensure_collection()
        self.store.ensure_collection()

        collection = self.client.get_collection("test_images")
        vector_params = collection.config.params.vectors
        self.assertEqual(vector_params.size, CLIP_VECTOR_SIZE)
        self.assertEqual(vector_params.distance, models.Distance.COSINE)

    def test_existing_collection_with_wrong_size_is_rejected(self):
        self.client.create_collection(
            collection_name="test_images",
            vectors_config=models.VectorParams(
                size=3,
                distance=models.Distance.COSINE,
            ),
        )

        with self.assertRaises(CollectionConfigurationError):
            self.store.ensure_collection()

    def test_upsert_and_search_returns_nearest_vector(self):
        self.store.ensure_collection()
        first_id = build_image_point_id("request-1", 1, 0)
        second_id = build_image_point_id("request-1", 1, 1)
        first_vector = [1.0] + [0.0] * (CLIP_VECTOR_SIZE - 1)
        second_vector = [0.0, 1.0] + [0.0] * (CLIP_VECTOR_SIZE - 2)
        query_vector = [0.9, 0.1] + [0.0] * (CLIP_VECTOR_SIZE - 2)

        self.store.upsert(first_id, first_vector, {"name": "first"})
        self.store.upsert(second_id, second_vector, {"name": "second"})

        result = self.store.search(query_vector, limit=1)

        self.assertEqual(result[0].id, first_id)
        self.assertEqual(result[0].payload["name"], "first")

    def test_upsert_replaces_existing_point(self):
        self.store.ensure_collection()
        point_id = build_image_point_id("request-1", 1, 0)
        vector = [1.0] + [0.0] * (CLIP_VECTOR_SIZE - 1)

        self.store.upsert(point_id, vector, {"version": 1})
        self.store.upsert(point_id, vector, {"version": 2})

        points = self.client.retrieve("test_images", ids=[point_id])
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0].payload["version"], 2)

    def test_wrong_vector_size_is_rejected(self):
        self.store.ensure_collection()

        with self.assertRaises(InvalidVectorError):
            self.store.upsert("invalid", [1.0, 0.0], {})

    def test_point_id_is_deterministic(self):
        first = build_image_point_id("request-1", 2, 3)
        repeated = build_image_point_id("request-1", 2, 3)
        different = build_image_point_id("request-1", 2, 4)

        self.assertEqual(first, repeated)
        self.assertNotEqual(first, different)


class QdrantConfigurationTests(unittest.TestCase):
    def test_in_memory_client_can_be_created(self):
        settings = QdrantSettings(
            url=None,
            api_key=None,
            local_path=":memory:",
            collection_name="test_images",
        )

        client = create_qdrant_client(settings)

        self.assertIsInstance(client, QdrantClient)

    def test_remote_and_local_modes_cannot_be_combined(self):
        settings = QdrantSettings(
            url="http://localhost:6333",
            api_key=None,
            local_path="./data/qdrant",
            collection_name="test_images",
        )

        with self.assertRaises(QdrantConfigurationError):
            create_qdrant_client(settings)

    def test_a_connection_mode_is_required(self):
        settings = QdrantSettings(
            url=None,
            api_key=None,
            local_path=None,
            collection_name="test_images",
        )

        with self.assertRaises(QdrantConfigurationError):
            create_qdrant_client(settings)


if __name__ == "__main__":
    unittest.main()
