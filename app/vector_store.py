import uuid
from functools import lru_cache

from qdrant_client import QdrantClient, models

from app.qdrant_config import QdrantSettings, create_qdrant_client


class CollectionConfigurationError(RuntimeError):
    pass


class InvalidVectorError(ValueError):
    pass


class ImageVectorStore:
    def __init__(self, client, collection_name, vector_size):
        self.client = client
        self.collection_name = collection_name
        self.vector_size = vector_size

    def ensure_collection(self):
        if not self.client.collection_exists(self.collection_name):
            self._create_collection_if_missing()

        self._validate_collection()

    def upsert(self, point_id, vector, payload):
        self._validate_vector(vector)
        self.client.upsert(
            collection_name=self.collection_name,
            points=[
                models.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload,
                )
            ],
            wait=True,
        )

    def search(self, query_vector, limit=10, score_threshold=None):
        self._validate_vector(query_vector)
        if limit < 1:
            raise ValueError("Search limit must be at least 1.")

        result = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=limit,
            score_threshold=score_threshold,
            with_payload=True,
        )
        return result.points

    def _create_collection_if_missing(self):
        try:
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_size,
                    distance=models.Distance.COSINE,
                ),
            )
        except Exception:
            if not self.client.collection_exists(self.collection_name):
                raise

    def _validate_collection(self):
        collection = self.client.get_collection(self.collection_name)
        vector_params = collection.config.params.vectors

        if isinstance(vector_params, dict):
            raise CollectionConfigurationError(
                "The image collection must use one unnamed vector."
            )

        if vector_params.size != self.vector_size:
            raise CollectionConfigurationError(
                f"Collection vector size is {vector_params.size}; "
                f"expected {self.vector_size}."
            )

        if vector_params.distance != models.Distance.COSINE:
            raise CollectionConfigurationError(
                "The image collection must use cosine distance."
            )

    def _validate_vector(self, vector):
        if len(vector) != self.vector_size:
            raise InvalidVectorError(
                f"Vector contains {len(vector)} values; "
                f"expected {self.vector_size}."
            )


def build_image_point_id(request_id, row_number, image_index):
    source = f"image_processor:{request_id}:{row_number}:{image_index}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, source))


@lru_cache(maxsize=1)
def get_image_vector_store():
    settings = QdrantSettings.from_env()
    client = create_qdrant_client(settings)
    store = ImageVectorStore(
        client=client,
        collection_name=settings.collection_name,
        vector_size=settings.vector_size,
    )
    store.ensure_collection()
    return store
