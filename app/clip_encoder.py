import io
import math
import os
from dataclasses import dataclass
from functools import lru_cache

import torch
import torch.nn.functional as functional
from PIL import Image

from app.qdrant_config import CLIP_VECTOR_SIZE


DEFAULT_CLIP_MODEL = "openai/clip-vit-base-patch32"


class ClipConfigurationError(ValueError):
    pass


class InvalidEmbeddingError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClipSettings:
    model_name: str = DEFAULT_CLIP_MODEL
    device: str = "cpu"

    @classmethod
    def from_env(cls):
        return cls(
            model_name=os.getenv("CLIP_MODEL_NAME", DEFAULT_CLIP_MODEL),
            device=os.getenv("CLIP_DEVICE", "cpu"),
        )


class ClipEncoder:
    def __init__(self, model, processor, device="cpu"):
        self.device = _resolve_device(device)
        self.model = model.to(self.device)
        self.model.eval()
        self.processor = processor

    def encode_image(self, image):
        rgb_image = _load_rgb_image(image)
        inputs = self.processor(images=rgb_image, return_tensors="pt")

        with torch.inference_mode():
            features = self.model.get_image_features(
                **_move_inputs(inputs, self.device)
            )

        return _normalize_embedding(features)

    def encode_text(self, text):
        if not isinstance(text, str) or not text.strip():
            raise ValueError("Text must not be empty.")

        inputs = self.processor(
            text=[text],
            return_tensors="pt",
            padding=True,
            truncation=True,
        )

        with torch.inference_mode():
            features = self.model.get_text_features(
                **_move_inputs(inputs, self.device)
            )

        return _normalize_embedding(features)


@lru_cache(maxsize=1)
def get_clip_encoder():
    settings = ClipSettings.from_env()

    # Importing lazily keeps web and worker startup independent of model loading.
    from transformers import CLIPModel, CLIPProcessor

    model = CLIPModel.from_pretrained(settings.model_name)
    processor = CLIPProcessor.from_pretrained(settings.model_name)
    return ClipEncoder(model, processor, settings.device)


def _resolve_device(device):
    normalized = device.strip().lower()
    if normalized == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")

    if normalized == "cuda" and not torch.cuda.is_available():
        raise ClipConfigurationError(
            "CLIP_DEVICE is cuda, but CUDA is not available."
        )

    try:
        return torch.device(normalized)
    except (RuntimeError, ValueError) as exc:
        raise ClipConfigurationError(
            f"Unsupported CLIP_DEVICE value: {device}."
        ) from exc


def _load_rgb_image(image):
    if isinstance(image, Image.Image):
        return image.convert("RGB")

    if isinstance(image, (bytes, bytearray)):
        image = io.BytesIO(image)

    try:
        with Image.open(image) as opened_image:
            return opened_image.convert("RGB")
    except (AttributeError, OSError, TypeError) as exc:
        raise ValueError("Image must be a Pillow image, bytes, or image file.") from exc


def _move_inputs(inputs, device):
    return {name: value.to(device) for name, value in inputs.items()}


def _normalize_embedding(features):
    if features.ndim != 2 or features.shape != (1, CLIP_VECTOR_SIZE):
        raise InvalidEmbeddingError(
            f"CLIP returned shape {tuple(features.shape)}; "
            f"expected (1, {CLIP_VECTOR_SIZE})."
        )

    normalized = functional.normalize(features, p=2, dim=-1)
    vector = normalized[0].detach().cpu().tolist()

    if not all(math.isfinite(value) for value in vector):
        raise InvalidEmbeddingError("CLIP returned non-finite values.")

    norm = math.sqrt(sum(value * value for value in vector))
    if not math.isclose(norm, 1.0, rel_tol=1e-5, abs_tol=1e-5):
        raise InvalidEmbeddingError("CLIP returned a zero-length embedding.")

    return vector
