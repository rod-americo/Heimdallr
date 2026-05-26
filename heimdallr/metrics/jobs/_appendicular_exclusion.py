"""Exclude appendicular tissue components from L3 slice measurements."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np
from scipy.ndimage import binary_dilation, generate_binary_structure, label as ndlabel


UPPER_APPENDICULAR_TOTAL_MASKS = (
    "humerus_left",
    "humerus_right",
    "scapula_left",
    "scapula_right",
    "clavicula_left",
    "clavicula_right",
)


def _ellipse_footprint(radius_mm: float, spacing_mm: tuple[float, float]) -> np.ndarray:
    radius = max(float(radius_mm), 0.0)
    spacing_x = max(float(spacing_mm[0]), 1e-6)
    spacing_y = max(float(spacing_mm[1]), 1e-6)
    radius_x = max(1, int(np.ceil(radius / spacing_x)))
    radius_y = max(1, int(np.ceil(radius / spacing_y)))
    x = np.arange(-radius_x, radius_x + 1, dtype=np.float32) * spacing_x
    y = np.arange(-radius_y, radius_y + 1, dtype=np.float32) * spacing_y
    xx, yy = np.meshgrid(x, y, indexing="ij")
    return (xx**2 + yy**2) <= radius**2


def load_upper_appendicular_mask_slice(
    artifacts_dir: Path,
    *,
    reference_shape: tuple[int, ...],
    slice_idx: int,
    spacing_z_mm: float | None = None,
    slice_half_window_mm: float = 80.0,
    mask_names: tuple[str, ...] = UPPER_APPENDICULAR_TOTAL_MASKS,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Load upper appendicular TotalSegmentator masks projected near one axial slice."""
    exclusion_slice = np.zeros(reference_shape[:2], dtype=bool)
    loaded_masks: list[str] = []
    skipped_masks: dict[str, str] = {}
    total_dir = artifacts_dir / "total"
    spacing_z = float(spacing_z_mm) if spacing_z_mm else None
    if spacing_z and spacing_z > 0 and slice_half_window_mm > 0:
        half_window_slices = int(np.ceil(float(slice_half_window_mm) / spacing_z))
    else:
        half_window_slices = 0
    slice_start = max(0, int(slice_idx) - half_window_slices)
    slice_end = min(int(reference_shape[2]), int(slice_idx) + half_window_slices + 1)

    for mask_name in mask_names:
        mask_path = total_dir / f"{mask_name}.nii.gz"
        if not mask_path.exists():
            continue
        mask_img = nib.load(str(mask_path))
        mask_data = np.asarray(mask_img.get_fdata()) > 0
        if mask_data.shape != reference_shape:
            skipped_masks[mask_name] = "shape_mismatch"
            continue
        exclusion_slice |= np.any(mask_data[:, :, slice_start:slice_end], axis=2)
        loaded_masks.append(mask_name)

    audit: dict[str, Any] = {
        "source_masks": loaded_masks,
        "skipped_masks": skipped_masks,
        "source_mask_pixels": int(np.count_nonzero(exclusion_slice)),
        "projection": {
            "center_slice_index": int(slice_idx),
            "slice_start": int(slice_start),
            "slice_end_exclusive": int(slice_end),
            "half_window_mm": float(slice_half_window_mm),
            "spacing_z_mm": spacing_z,
        },
    }
    return exclusion_slice, audit


def remove_appendicular_tissue_components(
    tissue_slice: np.ndarray,
    appendicular_slice: np.ndarray,
    *,
    spacing_mm: tuple[float, float],
    tissue_label: str,
    margin_mm: float = 35.0,
    max_removed_fraction: float = 0.45,
    enabled: bool = True,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Remove tissue components touching a dilated upper appendicular bone mask."""
    tissue_bool = np.asarray(tissue_slice, dtype=bool)
    appendicular_bool = np.asarray(appendicular_slice, dtype=bool)
    raw_pixels = int(np.count_nonzero(tissue_bool))
    audit: dict[str, Any] = {
        "method": "remove_tissue_components_touching_dilated_upper_appendicular_total_masks",
        "tissue_label": str(tissue_label),
        "enabled": bool(enabled),
        "applied": False,
        "margin_mm": float(margin_mm),
        "max_removed_fraction": float(max_removed_fraction),
        "raw_pixels": raw_pixels,
        "excluded_pixels": 0,
        "excluded_fraction": 0.0,
        "kept_pixels": raw_pixels,
        "removed_component_count": 0,
        "reason": None,
    }
    if not enabled:
        audit["reason"] = "disabled"
        return tissue_bool, audit
    if raw_pixels == 0:
        audit["reason"] = "empty_tissue_mask"
        return tissue_bool, audit
    if not appendicular_bool.any():
        audit["reason"] = "no_appendicular_mask_on_slice"
        return tissue_bool, audit

    footprint = _ellipse_footprint(margin_mm, spacing_mm)
    appendicular_support = binary_dilation(appendicular_bool, structure=footprint)
    labels, component_count = ndlabel(tissue_bool, structure=generate_binary_structure(2, 2))
    if component_count == 0:
        audit["reason"] = "empty_tissue_components"
        return tissue_bool, audit

    labels_to_remove = np.unique(labels[appendicular_support & tissue_bool])
    labels_to_remove = labels_to_remove[labels_to_remove > 0]
    if labels_to_remove.size == 0:
        audit["reason"] = "no_tissue_component_touches_appendicular_support"
        return tissue_bool, audit

    removal_mask = np.isin(labels, labels_to_remove)
    excluded_pixels = int(np.count_nonzero(removal_mask))
    excluded_fraction = excluded_pixels / raw_pixels
    if excluded_fraction > max(float(max_removed_fraction), 0.0):
        audit.update(
            {
                "removed_component_count": int(labels_to_remove.size),
                "excluded_pixels": excluded_pixels,
                "excluded_fraction": round(float(excluded_fraction), 4),
                "reason": "candidate_removal_exceeds_safety_limit",
            }
        )
        return tissue_bool, audit

    cleaned = tissue_bool & ~removal_mask
    kept_pixels = int(np.count_nonzero(cleaned))
    audit.update(
        {
            "applied": True,
            "excluded_pixels": excluded_pixels,
            "excluded_fraction": round(float(excluded_fraction), 4),
            "kept_pixels": kept_pixels,
            "removed_component_count": int(labels_to_remove.size),
            "reason": "appendicular_tissue_components_removed",
        }
    )
    return cleaned, audit
