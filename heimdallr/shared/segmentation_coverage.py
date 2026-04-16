"""Helpers to classify segmentation coverage from total-body masks."""

from __future__ import annotations

from pathlib import Path

import nibabel as nib
import numpy as np


SEGMENTATION_COVERAGE_UNKNOWN = "unknown"
SEGMENTATION_COVERAGE_PARTIAL = "partial"
SEGMENTATION_COVERAGE_CHEST_ONLY = "chest_only"
SEGMENTATION_COVERAGE_ABDOMEN_ONLY = "abdomen_only"
SEGMENTATION_COVERAGE_CHEST_ABDOMEN = "chest_abdomen"

_LUNG_MASK_NAMES = (
    "lung_upper_lobe_left.nii.gz",
    "lung_upper_lobe_right.nii.gz",
    "lung_middle_lobe_right.nii.gz",
    "lung_lower_lobe_left.nii.gz",
    "lung_lower_lobe_right.nii.gz",
)
_ABDOMINAL_MASK_NAMES = (
    "liver.nii.gz",
    "spleen.nii.gz",
    "pancreas.nii.gz",
    "kidney_left.nii.gz",
    "kidney_right.nii.gz",
)
_ABDOMINAL_CORE_MASK_NAMES = (
    "liver.nii.gz",
    "spleen.nii.gz",
    "pancreas.nii.gz",
)


def mask_complete_along_axis(mask: np.ndarray, axis: int) -> bool:
    """Return True when a 3D mask does not touch scan bounds along one axis."""
    mask_bool = np.asarray(mask, dtype=bool)
    if mask_bool.ndim != 3 or not np.any(mask_bool):
        return False
    axis = int(axis)
    if axis < 0 or axis >= mask_bool.ndim:
        return False
    reduce_axes = tuple(idx for idx in range(mask_bool.ndim) if idx != axis)
    occupied_indices = np.where(mask_bool.sum(axis=reduce_axes) > 0)[0]
    if len(occupied_indices) == 0:
        return False
    return int(occupied_indices[0]) > 0 and int(occupied_indices[-1]) < (mask_bool.shape[axis] - 1)


def mask_complete(mask: np.ndarray) -> bool:
    return mask_complete_along_axis(mask, axis=2)


def _load_complete_mask_names(total_artifacts_dir: Path) -> set[str]:
    complete = set()
    if not total_artifacts_dir.exists():
        return complete

    for mask_name in _LUNG_MASK_NAMES + _ABDOMINAL_MASK_NAMES:
        mask_path = total_artifacts_dir / mask_name
        if not mask_path.exists():
            continue
        try:
            image = nib.load(str(mask_path))
            data = image.get_fdata(dtype=np.float32)
        except Exception:
            continue
        if mask_complete(data > 0):
            complete.add(mask_name)
    return complete


def classify_segmentation_coverage(total_artifacts_dir: Path) -> str:
    """Classify whether a segmentation covered chest, abdomen, or both."""
    complete = _load_complete_mask_names(total_artifacts_dir)
    if not complete:
        return SEGMENTATION_COVERAGE_UNKNOWN

    chest_complete = all(name in complete for name in _LUNG_MASK_NAMES)
    abdominal_complete_count = sum(1 for name in _ABDOMINAL_MASK_NAMES if name in complete)
    abdomen_complete = (
        abdominal_complete_count >= 3
        and any(name in complete for name in _ABDOMINAL_CORE_MASK_NAMES)
    )

    if chest_complete and abdomen_complete:
        return SEGMENTATION_COVERAGE_CHEST_ABDOMEN
    if chest_complete:
        return SEGMENTATION_COVERAGE_CHEST_ONLY
    if abdomen_complete:
        return SEGMENTATION_COVERAGE_ABDOMEN_ONLY
    return SEGMENTATION_COVERAGE_PARTIAL
