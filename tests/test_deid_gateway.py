import io
import unittest
from unittest.mock import patch

from PIL import Image

from deid_gateway import (
    DeidReviewRequiredError,
    coarsen_age,
    deidentify_image_payload,
    sanitize_free_text,
)


class TestDeidGateway(unittest.TestCase):
    def test_coarsen_age_default(self):
        self.assertEqual(coarsen_age("43 years"), "40-44 years")
        self.assertEqual(coarsen_age("7 months"), "5-9 months")
        self.assertEqual(coarsen_age("12 dias"), "10-14 days")
        self.assertEqual(coarsen_age("92"), "90+ years")
        self.assertEqual(coarsen_age(""), "unknown age")

    def test_sanitize_free_text(self):
        text = "John 2026-01-31 MRN 123456 email a@b.com phone +1 555 333 2222"
        out = sanitize_free_text(text)
        self.assertIn("[REDACTED_DATE]", out)
        self.assertIn("[REDACTED_ID]", out)
        self.assertIn("[REDACTED_EMAIL]", out)
        self.assertIn("[REDACTED_PHONE]", out)

    def test_deidentify_regular_image(self):
        img = Image.new("RGB", (100, 100), (255, 255, 255))
        raw = io.BytesIO()
        img.save(raw, format="PNG")

        with patch(
            "deid_gateway._detect_text_boxes_ocr",
            return_value=([], {"ocr_available": True, "ocr_engine": "mock"}),
        ):
            result = deidentify_image_payload(raw.getvalue())
        self.assertEqual(result.media_type, "image/jpeg")
        self.assertFalse(result.details.get("pixel_redaction"))
        self.assertTrue(result.details.get("metadata_removed"))
        self.assertGreater(len(result.data), 0)
        self.assertFalse(result.review_required)

    def test_deidentify_blocks_when_ocr_detects_text(self):
        img = Image.new("RGB", (100, 100), (255, 255, 255))
        raw = io.BytesIO()
        img.save(raw, format="PNG")
        boxes = [{"x": 1, "y": 2, "w": 20, "h": 10, "text": "NAME", "confidence": 91.0}]

        with patch.dict("os.environ", {"DEID_OCR_ACTION": "block"}):
            with patch(
                "deid_gateway._detect_text_boxes_ocr",
                return_value=(boxes, {"ocr_available": True, "ocr_engine": "mock"}),
            ):
                with self.assertRaises(DeidReviewRequiredError):
                    deidentify_image_payload(raw.getvalue())


if __name__ == "__main__":
    unittest.main()
