import unittest

from fastapi import HTTPException

from app.validation import validate_csv


class ValidateCsvTests(unittest.TestCase):
    def test_accepts_valid_csv(self):
        csv_content = (
            "Sr. No,Product Name,Input Image URLs\n"
            "item-1,Beach Photo,https://example.com/photo.jpg\n"
        )

        self.assertIsNone(validate_csv(csv_content))

    def test_rejects_wrong_header_order(self):
        csv_content = (
            "Product Name,Sr. No,Input Image URLs\n"
            "Beach Photo,item-1,https://example.com/photo.jpg\n"
        )

        with self.assertRaises(HTTPException) as context:
            validate_csv(csv_content)

        self.assertEqual(context.exception.status_code, 400)

    def test_accepts_image_url_query_parameters(self):
        csv_content = (
            "Sr. No,Product Name,Input Image URLs\n"
            "item-1,Beach Photo,https://example.com/photo.jpg?version=2\n"
        )

        self.assertIsNone(validate_csv(csv_content))

    def test_rejects_duplicate_serial_number(self):
        csv_content = (
            "Sr. No,Product Name,Input Image URLs\n"
            "item-1,First,https://example.com/first.jpg\n"
            "item-1,Second,https://example.com/second.jpg\n"
        )

        with self.assertRaises(HTTPException) as context:
            validate_csv(csv_content)

        self.assertIn("Duplicate serial number", context.exception.detail)

    def test_rejects_empty_csv(self):
        with self.assertRaises(HTTPException) as context:
            validate_csv("Sr. No,Product Name,Input Image URLs\n")

        self.assertEqual(context.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
