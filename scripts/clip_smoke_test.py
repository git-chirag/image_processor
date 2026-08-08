import math

from PIL import Image

from app.clip_encoder import get_clip_encoder


def main():
    encoder = get_clip_encoder()
    image_vector = encoder.encode_image(Image.new("RGB", (224, 224), "gold"))
    text_vector = encoder.encode_text("a solid gold image")
    similarity = sum(
        image_value * text_value
        for image_value, text_value in zip(image_vector, text_vector)
    )

    print(f"Image dimensions: {len(image_vector)}")
    print(f"Text dimensions: {len(text_vector)}")
    print(f"Image norm: {_norm(image_vector):.4f}")
    print(f"Text norm: {_norm(text_vector):.4f}")
    print(f"Cosine similarity: {similarity:.4f}")


def _norm(vector):
    return math.sqrt(sum(value * value for value in vector))


if __name__ == "__main__":
    main()
