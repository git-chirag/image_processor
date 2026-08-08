from qdrant_client import QdrantClient

from app.qdrant_config import CLIP_VECTOR_SIZE, DEFAULT_COLLECTION_NAME
from app.vector_store import ImageVectorStore, build_image_point_id


def main():
    client = QdrantClient(":memory:")
    store = ImageVectorStore(
        client=client,
        collection_name=DEFAULT_COLLECTION_NAME,
        vector_size=CLIP_VECTOR_SIZE,
    )
    store.ensure_collection()

    first_vector = [1.0] + [0.0] * (CLIP_VECTOR_SIZE - 1)
    second_vector = [0.0, 1.0] + [0.0] * (CLIP_VECTOR_SIZE - 2)
    query_vector = [0.9, 0.1] + [0.0] * (CLIP_VECTOR_SIZE - 2)

    store.upsert(
        point_id=build_image_point_id("sample", 1, 0),
        vector=first_vector,
        payload={"name": "first sample image"},
    )
    store.upsert(
        point_id=build_image_point_id("sample", 1, 1),
        vector=second_vector,
        payload={"name": "second sample image"},
    )

    result = store.search(query_vector, limit=1)[0]
    print(
        "Nearest point:",
        result.payload["name"],
        f"(cosine score={result.score:.4f})",
    )


if __name__ == "__main__":
    main()
