"""Helpers for patient-name normalization in user-facing views."""

from __future__ import annotations

import re


def _normalize_whitespace(name: str) -> str:
    return re.sub(r"\s+", " ", str(name).replace("^", " ")).strip()


def _normalize_dicom_caret_name(name: str) -> str:
    parts = [re.sub(r"\s+", " ", part).strip() for part in str(name).split("^")]
    parts = [part for part in parts if part]
    if len(parts) < 2:
        return _normalize_whitespace(name)
    return " ".join(parts[1:] + parts[:1])


def normalize_patient_name_display(name: str, profile: str = "default") -> str:
    """Return a UI-friendly patient name while preserving the raw source elsewhere."""
    cleaned = _normalize_whitespace(name)
    if not cleaned:
        return ""

    profile_key = (profile or "default").strip().lower()
    if profile_key == "dicom_caret":
        return _normalize_dicom_caret_name(name)

    return cleaned
