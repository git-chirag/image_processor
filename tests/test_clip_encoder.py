import io
import math
import unittest

import torch
from PIL import Image

from app.clip_encoder import ClipEncoder, InvalidEmbeddingError
from app.qdrant_config import CLIP_VECTOR_SIZE


class FakeProcessor:
    def __init__(self):
        self.last_image = None
        self.last_text = None

    def __call__(self, **kwargs):
        self.last_image = kwargs.get("images")
        self.last_text = kwargs.get("text")
        return {"sample": torch.tensor([[1.0]])}


class FakeModel:
    def __init__(self, output_size=CLIP_VECTOR_SIZE):
        self.output_size = output_size
        self.device = None
        self.is_evaluation = False

    def to(self, device):
        self.device = device
        return self

    def eval(self):
        self.is_evaluation = True
        return self

    def get_image_features(self, **_inputs):
        return torch.arange(1, self.output_size + 1, dtype=torch.float32)[None, :]

    def get_text_features(self, **_inputs):
        return torch.ones((1, self.output_size), dtype=torch.float32)


class ClipEncoderTests(unittest.TestCase):
    def setUp(self):
        self.model = FakeModel()
        self.processor = FakeProcessor()
        self.encoder = ClipEncoder(self.model, self.processor)

    def test_model_is_prepared_for_inference(self):
        self.assertEqual(self.model.device, torch.device("cpu"))
        self.assertTrue(self.model.is_evaluation)

    def test_image_embedding_is_512_dimensional_and_normalized(self):
        vector = self.encoder.encode_image(Image.new("RGB", (8, 8), "red"))

        self.assertEqual(len(vector), CLIP_VECTOR_SIZE)
        self.assertAlmostEqual(_norm(vector), 1.0, places=5)
        self.assertEqual(self.processor.last_image.mode, "RGB")

    def test_image_bytes_are_supported_and_converted_to_rgb(self):
        image_bytes = io.BytesIO()
        Image.new("L", (8, 8), 128).save(image_bytes, format="PNG")

        self.encoder.encode_image(image_bytes.getvalue())

        self.assertEqual(self.processor.last_image.mode, "RGB")

    def test_text_embedding_is_512_dimensional_and_normalized(self):
        vector = self.encoder.encode_text("a golden retriever on a beach")

        self.assertEqual(len(vector), CLIP_VECTOR_SIZE)
        self.assertAlmostEqual(_norm(vector), 1.0, places=5)
        self.assertEqual(
            self.processor.last_text,
            ["a golden retriever on a beach"],
        )

    def test_empty_text_is_rejected(self):
        with self.assertRaises(ValueError):
            self.encoder.encode_text("   ")

    def test_wrong_embedding_size_is_rejected(self):
        encoder = ClipEncoder(FakeModel(output_size=128), FakeProcessor())

        with self.assertRaises(InvalidEmbeddingError):
            encoder.encode_text("sample")


def _norm(vector):
    return math.sqrt(sum(value * value for value in vector))


if __name__ == "__main__":
    unittest.main()
