"""Helpers for opportunistic hepatic steatosis support metrics."""

from __future__ import annotations


def estimate_pdff_from_unenhanced_ct_hu(hu_mean: float | None) -> float | None:
    """Estimate MRI PDFF (%) from unenhanced CT mean attenuation (HU)."""
    if hu_mean is None:
        return None
    try:
        value = float(hu_mean)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, (-0.58 * value) + 38.2), 2)
