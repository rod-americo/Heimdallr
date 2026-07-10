"""Helpers for opportunistic hepatic steatosis support metrics."""

from __future__ import annotations

import math


STEATOSIS_KVP_RANGE = (115.0, 125.0)


def estimate_pdff_from_unenhanced_ct_hu(hu_mean: float | None) -> float | None:
    """Estimate MRI PDFF (%) from unenhanced CT mean attenuation (HU)."""
    if hu_mean is None:
        return None
    try:
        value = float(hu_mean)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, (-0.58 * value) + 38.2), 2)


def assess_hepatic_steatosis(
    liver_hu: float | None,
    spleen_hu: float | None,
    kvp: float | None,
) -> dict[str, float | int | str | None] | None:
    """Assess steatosis for the parenchymal overlay using the requested CT rule."""
    try:
        liver_value = float(liver_hu) if liver_hu is not None else None
    except (TypeError, ValueError):
        liver_value = None
    if liver_value is None or not math.isfinite(liver_value):
        return None

    try:
        spleen_value = float(spleen_hu) if spleen_hu is not None else None
    except (TypeError, ValueError):
        spleen_value = None
    if spleen_value is not None and not math.isfinite(spleen_value):
        spleen_value = None
    ratio = liver_value / spleen_value if spleen_value not in (None, 0.0) else None

    try:
        kvp_value = float(kvp) if kvp is not None else None
    except (TypeError, ValueError):
        kvp_value = None
    if kvp_value is None or not math.isfinite(kvp_value) or not (
        STEATOSIS_KVP_RANGE[0] <= kvp_value <= STEATOSIS_KVP_RANGE[1]
    ):
        return {
            "status": "kvp_out_of_range",
            "kvp": kvp_value,
            "liver_hu": liver_value,
            "spleen_hu": spleen_value,
            "liver_to_spleen_ratio": ratio,
            "estimated_percent": None,
        }

    if liver_value >= 50.0 or (ratio is not None and ratio > 1.0):
        status = "normal"
        estimated_percent = None
    else:
        status = "estimated"
        raw_percent = max(0.0, (-0.58 * liver_value) + 38.2)
        estimated_percent = int(math.floor(raw_percent + 0.5))

    return {
        "status": status,
        "kvp": kvp_value,
        "liver_hu": liver_value,
        "spleen_hu": spleen_value,
        "liver_to_spleen_ratio": ratio,
        "estimated_percent": estimated_percent,
    }
