from io import BytesIO

from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image, ImageDraw
from qdrant_client import QdrantClient

from app.clip_encoder import get_clip_encoder
from app.image_indexer import index_processed_image
from app.qdrant_config import CLIP_VECTOR_SIZE, DEFAULT_COLLECTION_NAME
from app.search_api import router
from app.search_service import ImageSearchService, get_image_search_service
from app.vector_store import ImageVectorStore


def main():
    qdrant_client = QdrantClient(path="./data/qdrant")
    vector_store = ImageVectorStore(
        client=qdrant_client,
        collection_name=DEFAULT_COLLECTION_NAME,
        vector_size=CLIP_VECTOR_SIZE,
    )
    vector_store.ensure_collection()
    encoder = get_clip_encoder()

    indexed_images = [
        ("Red circle", _shape_image("circle", "red", offset=0)),
        ("Blue square", _shape_image("square", "blue", offset=0)),
        ("Green triangle", _shape_image("triangle", "green", offset=0)),
    ]
    for image_index, (name, image_content) in enumerate(indexed_images):
        index_processed_image(
            request_id="visual-search-smoke-test",
            row_number=image_index + 1,
            image_index=0,
            image_url=f"local://{name.lower().replace(' ', '-')}",
            compressed_url=f"local://processed/{image_index}",
            image_content=image_content,
            product_name=name,
            encoder=encoder,
            vector_store=vector_store,
        )

    service = ImageSearchService(encoder=encoder, vector_store=vector_store)
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_image_search_service] = lambda: service

    query_image = _shape_image("circle", "red", offset=8)
    with TestClient(app) as client:
        response = client.post(
            "/api/v1/images/search-by-image",
            params={"limit": 3},
            files={"file": ("shifted-red-circle.png", query_image, "image/png")},
        )
        duplicate_response = client.post(
            "/api/v1/images/detect-duplicates",
            params={"threshold": 0.95},
            files={"file": ("shifted-red-circle.png", query_image, "image/png")},
        )

    response.raise_for_status()
    results = response.json()["results"]
    for position, result in enumerate(results, start=1):
        print(
            f"{position}. {result['product_name']} "
            f"(cosine score={result['score']:.4f})"
        )

    if not results or results[0]["product_name"] != "Red circle":
        raise RuntimeError("Expected the red circle to be the closest image.")

    print("Visual search passed: the modified red circle ranked first.")

    duplicate_response.raise_for_status()
    duplicate_result = duplicate_response.json()
    if not duplicate_result["is_duplicate"]:
        raise RuntimeError("Expected the modified red circle to be a duplicate.")
    print(
        "Duplicate detection passed: "
        f"{len(duplicate_result['matches'])} match(es) exceeded 0.95."
    )
    qdrant_client.close()


def _shape_image(shape, color, offset):
    image = Image.new("RGB", (224, 224), "white")
    draw = ImageDraw.Draw(image)

    if shape == "circle":
        draw.ellipse(
            (40 + offset, 40, 184 + offset, 184),
            fill=color,
        )
    elif shape == "square":
        draw.rectangle((40, 40, 184, 184), fill=color)
    elif shape == "triangle":
        draw.polygon(((112, 32), (32, 192), (192, 192)), fill=color)
    else:
        raise ValueError(f"Unsupported shape: {shape}")

    output = BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


if __name__ == "__main__":
    main()
