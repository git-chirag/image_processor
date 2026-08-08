from app.clip_encoder import get_clip_encoder
from app.vector_store import build_image_point_id, get_image_vector_store


def index_processed_image(
    request_id,
    row_number,
    image_index,
    image_url,
    compressed_url,
    image_content,
    product_name,
    encoder=None,
    vector_store=None,
):
    encoder = encoder or get_clip_encoder()
    vector_store = vector_store or get_image_vector_store()

    vector = encoder.encode_image(image_content)
    point_id = build_image_point_id(request_id, row_number, image_index)
    vector_store.upsert(
        point_id=point_id,
        vector=vector,
        payload={
            "request_id": request_id,
            "row_number": row_number,
            "image_index": image_index,
            "product_name": product_name,
            "original_url": image_url,
            "compressed_url": compressed_url,
        },
    )
    return point_id
