"""
De-identification gateway for outbound model payloads.

This module enforces two controls before any external model call:
1) Pixel de-identification: OCR-based burned-in text detection.
2) Metadata de-identification: scrub direct identifiers from text fields.
"""

from __future__ import annotations

import io
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pydicom
from PIL import Image, ImageDraw


_RE_EMAIL = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b")
_RE_PHONE = re.compile(r"\+?\d[\d\-\s\(\)]{7,}\d")
_RE_DATE = re.compile(
    r"\b(?:\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}|\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})\b"
)
_RE_LONG_ID = re.compile(r"\b\d{6,}\b")
_RE_AGE = re.compile(
    r"\b(\d{1,3})\s*(day|days|dia|dias|month|months|mes|meses|mês|mêses|year|years|ano|anos)?\b",
    re.IGNORECASE,
)


@dataclass
class DeidImageResult:
    data: bytes
    media_type: str
    review_required: bool
    bounding_boxes: List[Dict[str, Any]]
    details: Dict[str, Any]


class DeidReviewRequiredError(Exception):
    def __init__(self, details: Dict[str, Any]) -> None:
        super().__init__("De-identification review required before external call.")
        self.details = details


def _parse_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _parse_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _load_ocr_engine():
    try:
        import pytesseract  # type: ignore

        return pytesseract
    except Exception:
        return None


def _detect_text_boxes_ocr(image: Image.Image) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    engine = _load_ocr_engine()
    if engine is None:
        return [], {"ocr_available": False, "ocr_engine": "none"}

    min_conf = _parse_float("DEID_OCR_MIN_CONFIDENCE", 40.0)
    min_chars = _parse_int("DEID_OCR_MIN_CHARS", 2)
    ocr_config = os.getenv("DEID_OCR_CONFIG", "--psm 6")

    try:
        data = engine.image_to_data(
            image,
            output_type=engine.Output.DICT,
            config=ocr_config,
        )
    except Exception as exc:
        return [], {
            "ocr_available": False,
            "ocr_engine": "pytesseract",
            "ocr_error": str(exc),
        }

    boxes: List[Dict[str, Any]] = []
    total = len(data.get("text", []))
    for idx in range(total):
        raw_text = (data["text"][idx] or "").strip()
        if len(raw_text) < min_chars:
            continue
        try:
            conf = float(data["conf"][idx])
        except Exception:
            conf = -1.0
        if conf < min_conf:
            continue
        x = int(data["left"][idx])
        y = int(data["top"][idx])
        w = int(data["width"][idx])
        h = int(data["height"][idx])
        if w <= 0 or h <= 0:
            continue
        boxes.append(
            {
                "x": x,
                "y": y,
                "w": w,
                "h": h,
                "text": raw_text,
                "confidence": round(conf, 2),
            }
        )

    return boxes, {"ocr_available": True, "ocr_engine": "pytesseract"}


def _mask_ocr_boxes(image: Image.Image, boxes: List[Dict[str, Any]]) -> Image.Image:
    image = image.convert("RGB")
    draw = ImageDraw.Draw(image)
    for box in boxes:
        x = box["x"]
        y = box["y"]
        w = box["w"]
        h = box["h"]
        draw.rectangle((x, y, x + w, y + h), fill=(0, 0, 0))
    return image


def _dicom_to_rgb(ds: pydicom.Dataset) -> Image.Image:
    arr = ds.pixel_array.astype(np.float32)

    slope = float(getattr(ds, "RescaleSlope", 1.0))
    intercept = float(getattr(ds, "RescaleIntercept", 0.0))
    arr = arr * slope + intercept

    wc = getattr(ds, "WindowCenter", None)
    ww = getattr(ds, "WindowWidth", None)
    if wc is not None and ww is not None:
        wc = float(wc[0] if hasattr(wc, "__len__") else wc)
        ww = float(ww[0] if hasattr(ww, "__len__") else ww)
        lo, hi = wc - ww / 2, wc + ww / 2
        arr = np.clip(arr, lo, hi)

    arr -= arr.min()
    arr /= (arr.max() + 1e-6)
    arr = (arr * 255).astype(np.uint8)
    return Image.fromarray(arr).convert("RGB")


def deidentify_image_payload(file_bytes: bytes) -> DeidImageResult:
    """
    Convert payload to clean JPEG and enforce OCR-based review on burned-in text.

    - DICOM input: decoded to pixel data, metadata is not propagated.
    - PNG/JPG input: EXIF and embedded metadata are dropped on re-encode.
    """
    action = os.getenv("DEID_OCR_ACTION", "block").strip().lower()
    if action not in {"block", "mask", "allow"}:
        action = "block"

    review_required = False
    boxes: List[Dict[str, Any]] = []
    details: Dict[str, Any] = {
        "metadata_removed": True,
        "pixel_redaction": False,
        "ocr_action": action,
    }

    image: Image.Image
    # Try DICOM first.
    try:
        dicom_ds = pydicom.dcmread(io.BytesIO(file_bytes), force=True)
        if hasattr(dicom_ds, "PixelData"):
            image = _dicom_to_rgb(dicom_ds)
            details["input_format"] = "dicom"
        else:
            raise ValueError("DICOM without PixelData.")
    except Exception:
        # Fallback to regular image formats.
        try:
            image = Image.open(io.BytesIO(file_bytes)).convert("RGB")
            details["input_format"] = "image"
        except Exception as exc:
            raise ValueError(f"Unsupported image payload: {exc}") from exc

    boxes, ocr_meta = _detect_text_boxes_ocr(image)
    details.update(ocr_meta)
    review_required = len(boxes) > 0
    details["review_required"] = review_required
    details["bounding_boxes"] = boxes
    details["ocr_matches"] = len(boxes)

    if review_required and action == "mask":
        image = _mask_ocr_boxes(image, boxes)
        details["pixel_redaction"] = True

    if review_required and action == "block":
        raise DeidReviewRequiredError(details)

    out = io.BytesIO()
    image.save(out, format="JPEG", quality=95, optimize=True)
    return DeidImageResult(
        data=out.getvalue(),
        media_type="image/jpeg",
        review_required=review_required,
        bounding_boxes=boxes,
        details=details,
    )


def sanitize_free_text(value: str | None) -> str:
    """
    Remove common direct identifier patterns from free text metadata.
    """
    if not value:
        return ""
    text = value.strip()
    text = _RE_EMAIL.sub("[REDACTED_EMAIL]", text)
    text = _RE_DATE.sub("[REDACTED_DATE]", text)
    text = _RE_PHONE.sub("[REDACTED_PHONE]", text)
    text = _RE_LONG_ID.sub("[REDACTED_ID]", text)
    return text


def coarsen_age(value: str | None) -> str:
    """
    Coarsen age into bands to reduce re-identification risk.
    """
    if not value:
        return "unknown age"

    match = _RE_AGE.search(value or "")
    if not match:
        return sanitize_free_text(value)

    age = int(match.group(1))
    unit_raw = (match.group(2) or "years").lower()
    unit_map = {
        "day": "days",
        "days": "days",
        "dia": "days",
        "dias": "days",
        "month": "months",
        "months": "months",
        "mes": "months",
        "meses": "months",
        "mês": "months",
        "mêses": "months",
        "year": "years",
        "years": "years",
        "ano": "years",
        "anos": "years",
    }
    unit = unit_map.get(unit_raw, "years")

    if age >= 90:
        return "90+ years"
    low = (age // 5) * 5
    high = low + 4
    return f"{low}-{high} {unit}"


def sanitize_outbound_metadata(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Apply metadata sanitization to known outbound fields.
    """
    clean = dict(payload)
    details: Dict[str, Any] = {"metadata_redaction": True}

    if "age" in clean:
        clean["age"] = coarsen_age(str(clean.get("age", "")))
        details["age_coarsened"] = True

    for key in ("identificador", "patient_name", "accession", "mrn", "study_uid"):
        if key in clean and clean[key] is not None:
            clean[key] = sanitize_free_text(str(clean[key]))

    return clean, details
