import unittest
from io import BytesIO

from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from app.search_api import router
from app.search_service import get_image_search_service


class FakeSearchService:
    def __init__(self, results=None, error=None):
        self.results = results or []
        self.error = error
        self.arguments = None

    def search(self, **kwargs):
        self.arguments = kwargs
        if self.error:
            raise self.error
        return self.results

    def search_by_image(self, **kwargs):
        self.arguments = kwargs
        if self.error:
            raise self.error
        return self.results


class ImageSearchApiTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(router)

    def test_search_endpoint_returns_ranked_images(self):
        service = FakeSearchService(results=[{
            "point_id": "point-1",
            "score": 0.875,
            "request_id": "request-1",
            "row_number": 2,
            "image_index": 3,
            "product_name": "Dog poster",
            "original_url": "https://source.example/dog.jpg",
            "compressed_url": "https://bucket.example/dog.jpg",
        }])
        self.app.dependency_overrides[get_image_search_service] = lambda: service

        with TestClient(self.app) as client:
            response = client.get(
                "/api/v1/images/search",
                params={"q": " golden retriever ", "limit": 5},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["query"], "golden retriever")
        self.assertEqual(response.json()["results"][0]["point_id"], "point-1")
        self.assertEqual(service.arguments["limit"], 5)

    def test_limit_outside_allowed_range_is_rejected(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/api/v1/images/search",
                params={"q": "dog", "limit": 51},
            )

        self.assertEqual(response.status_code, 422)

    def test_blank_query_is_rejected(self):
        with TestClient(self.app) as client:
            response = client.get(
                "/api/v1/images/search",
                params={"q": "   "},
            )

        self.assertEqual(response.status_code, 422)

    def test_search_dependency_failure_returns_503(self):
        service = FakeSearchService(error=RuntimeError("Qdrant unavailable"))
        self.app.dependency_overrides[get_image_search_service] = lambda: service

        with TestClient(self.app) as client:
            response = client.get(
                "/api/v1/images/search",
                params={"q": "dog"},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json()["detail"],
            "Image search is temporarily unavailable.",
        )

    def test_visual_search_accepts_an_image_upload(self):
        service = FakeSearchService()
        self.app.dependency_overrides[get_image_search_service] = lambda: service

        with TestClient(self.app) as client:
            response = client.post(
                "/api/v1/images/search-by-image",
                params={"limit": 4, "score_threshold": 0.75},
                files={"file": ("query.png", _png_bytes(), "image/png")},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"filename": "query.png", "results": []})
        self.assertEqual(service.arguments["limit"], 4)
        self.assertEqual(service.arguments["score_threshold"], 0.75)
        self.assertTrue(service.arguments["image_content"])

    def test_visual_search_rejects_unsupported_content_type(self):
        with TestClient(self.app) as client:
            response = client.post(
                "/api/v1/images/search-by-image",
                files={"file": ("query.gif", b"GIF89a", "image/gif")},
            )

        self.assertEqual(response.status_code, 415)

    def test_visual_search_rejects_invalid_image_content(self):
        with TestClient(self.app) as client:
            response = client.post(
                "/api/v1/images/search-by-image",
                files={"file": ("query.png", b"not an image", "image/png")},
            )

        self.assertEqual(response.status_code, 400)


def _png_bytes():
    output = BytesIO()
    Image.new("RGB", (8, 8), "gold").save(output, format="PNG")
    return output.getvalue()


if __name__ == "__main__":
    unittest.main()
