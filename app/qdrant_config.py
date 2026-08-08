import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from qdrant_client import QdrantClient


CLIP_VECTOR_SIZE = 512
DEFAULT_COLLECTION_NAME = "image_embeddings_clip_v1"


class QdrantConfigurationError(ValueError):
    pass


@dataclass(frozen=True)
class QdrantSettings:
    url: Optional[str]
    api_key: Optional[str]
    local_path: Optional[str]
    collection_name: str
    vector_size: int = CLIP_VECTOR_SIZE
    timeout_seconds: int = 10

    @classmethod
    def from_env(cls):
        load_dotenv()
        return cls(
            url=_optional_env("QDRANT_URL"),
            api_key=_optional_env("QDRANT_API_KEY"),
            local_path=_optional_env("QDRANT_LOCAL_PATH"),
            collection_name=(
                _optional_env("QDRANT_COLLECTION")
                or DEFAULT_COLLECTION_NAME
            ),
        )


def create_qdrant_client(settings):
    if settings.url and settings.local_path:
        raise QdrantConfigurationError(
            "Set either QDRANT_URL or QDRANT_LOCAL_PATH, not both."
        )

    if settings.url:
        return QdrantClient(
            url=settings.url,
            api_key=settings.api_key,
            timeout=settings.timeout_seconds,
        )

    if settings.local_path == ":memory:":
        return QdrantClient(":memory:")

    if settings.local_path:
        return QdrantClient(path=settings.local_path)

    raise QdrantConfigurationError(
        "Set QDRANT_URL for a server or QDRANT_LOCAL_PATH for local mode."
    )


def _optional_env(name):
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None
