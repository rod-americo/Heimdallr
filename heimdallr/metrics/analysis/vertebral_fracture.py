#!/usr/bin/env python3
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Heuristic vertebral fracture screening helpers.

The functions in this module are intentionally pure and deterministic so that
they can be reused by metrics jobs, offline validation scripts, and tests.

The workflow is:
1. Normalize a vertebral mask.
2. Isolate a stable vertebral body core using distance-based erosion.
3. Infer the vertebral axes from the mask geometry when the caller does not
   provide them explicitly.
4. Estimate anterior, middle, and posterior heights in millimeters.
5. Convert the morphometry into a conservative triage label.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy.ndimage import binary_dilation, distance_transform_edt, label as ndlabel


ArrayLike = Any

DEFAULT_SPACING_MM = (1.0, 1.0, 1.0)
DEFAULT_CORE_RADIUS_MM = 2.5
DEFAULT_RESTORE_RADIUS_MM = 1.5
DEFAULT_EDGE_FRACTION = 0.18
DEFAULT_SMOOTHING_WINDOW = 3
VERTEBRA_LEVELS = tuple(
    [
        *(f"C{i}" for i in range(1, 8)),
        *(f"T{i}" for i in range(1, 13)),
        *(f"L{i}" for i in range(1, 6)),
        *(f"S{i}" for i in range(1, 5)),
    ]
)


def _normalize_spacing(spacing_mm: Any) -> tuple[float, float, float]:
    if spacing_mm is None:
        return DEFAULT_SPACING_MM

    values = list(spacing_mm)
    while len(values) < 3:
        values.append(1.0)

    normalized = []
    for value in values[:3]:
        try:
            normalized.append(max(float(value), 1e-6))
        except (TypeError, ValueError):
            normalized.append(1.0)
    return tuple(normalized)


def _as_bool_mask(mask_data: ArrayLike) -> np.ndarray:
    mask = np.asarray(mask_data) > 0
    if mask.ndim != 3:
        raise ValueError("vertebral mask must be a 3D array")
    return mask


def _largest_connected_component(mask: np.ndarray) -> np.ndarray:
    mask = np.asarray(mask) > 0
    if not np.any(mask):
        return mask

    structure = np.ones((3, 3, 3), dtype=np.uint8)
    labeled, num = ndlabel(mask, structure=structure)
    if num <= 1:
        return mask

    sizes = np.bincount(labeled.ravel())
    sizes[0] = 0
    largest_label = int(np.argmax(sizes))
    return labeled == largest_label


def _component_voxel_count(mask: np.ndarray) -> int:
    return int(np.count_nonzero(np.asarray(mask) > 0))


def _crop_to_mask_bbox(mask: np.ndarray, padding_voxels: int = 6) -> tuple[np.ndarray, tuple[slice, slice, slice]]:
    mask_bool = np.asarray(mask, dtype=bool)
    coords = np.argwhere(mask_bool)
    if coords.size == 0:
        full = (slice(0, mask_bool.shape[0]), slice(0, mask_bool.shape[1]), slice(0, mask_bool.shape[2]))
        return mask_bool.copy(), full

    mins = np.maximum(coords.min(axis=0) - int(padding_voxels), 0)
    maxs = np.minimum(coords.max(axis=0) + int(padding_voxels) + 1, np.asarray(mask_bool.shape))
    crop_slices = tuple(slice(int(start), int(stop)) for start, stop in zip(mins, maxs, strict=True))
    return np.asarray(mask_bool[crop_slices], dtype=bool), crop_slices


def _restore_from_crop(cropped_mask: np.ndarray, full_shape: tuple[int, int, int], crop_slices: tuple[slice, slice, slice]) -> np.ndarray:
    restored = np.zeros(full_shape, dtype=bool)
    restored[crop_slices] = np.asarray(cropped_mask, dtype=bool)
    return restored


def _crop_reoriented_to_dominant_ap_span(
    reoriented_mask: np.ndarray,
    *,
    min_area_fraction: float = 0.35,
    padding_slices: int = 1,
) -> np.ndarray:
    mask = np.asarray(reoriented_mask, dtype=bool)
    if mask.ndim != 3 or not np.any(mask):
        return mask

    area_profile = mask.sum(axis=(1, 2)).astype(np.float64)
    if area_profile.size == 0:
        return mask

    max_area = float(np.max(area_profile))
    if max_area <= 0.0:
        return mask

    threshold = max(1.0, float(min_area_fraction) * max_area)
    support = area_profile >= threshold
    if not np.any(support):
        return mask

    peak_index = int(np.argmax(area_profile))
    left = peak_index
    right = peak_index
    while left > 0 and support[left - 1]:
        left -= 1
    while right < (support.size - 1) and support[right + 1]:
        right += 1

    left = max(0, left - int(padding_slices))
    right = min(mask.shape[0] - 1, right + int(padding_slices))
    if right <= left:
        return mask

    cropped = np.zeros_like(mask, dtype=bool)
    cropped[left : right + 1] = mask[left : right + 1]
    return cropped


def _moving_average(values: np.ndarray, window: int) -> np.ndarray:
    if window <= 1 or values.size == 0:
        return values.astype(np.float64, copy=False)

    window = min(int(window), int(values.size))
    if window <= 1:
        return values.astype(np.float64, copy=False)

    kernel = np.ones(window, dtype=np.float64) / float(window)
    return np.convolve(values.astype(np.float64, copy=False), kernel, mode="same")


def _clamp01(value: float) -> float:
    return float(min(1.0, max(0.0, value)))


def vertebra_level_index(level: str | None) -> int | None:
    normalized = str(level or "").strip().upper()
    if not normalized:
        return None
    try:
        return VERTEBRA_LEVELS.index(normalized)
    except ValueError:
        return None


def _infer_axes(mask: np.ndarray, ap_axis: int | None = None, si_axis: int | None = None) -> dict[str, Any]:
    mask = np.asarray(mask) > 0
    spans = []
    coords = np.where(mask)
    for axis in range(3):
        axis_coords = coords[axis]
        if axis_coords.size == 0:
            spans.append(0)
        else:
            spans.append(int(np.ptp(axis_coords) + 1))

    if si_axis is None:
        si_axis = int(np.argmax(spans)) if np.any(spans) else 2

    remaining = [axis for axis in range(3) if axis != si_axis]
    if ap_axis is None:
        projection = mask.any(axis=si_axis)
        if projection.ndim != 2 or not np.any(projection):
            ap_axis = remaining[0]
            confidence = 0.0
        else:
            spans_2d = []
            for axis_index in (0, 1):
                coords = np.argwhere(projection)
                if coords.size == 0:
                    spans_2d.append(0)
                else:
                    spans_2d.append(int(np.ptp(coords[:, axis_index]) + 1))
            ap_axis = remaining[int(np.argmin(spans_2d))]
            width = max(spans_2d)
            confidence = 0.0 if width <= 0 else _clamp01(abs(spans_2d[0] - spans_2d[1]) / float(width))
    else:
        confidence = 1.0

    lateral_axis = [axis for axis in range(3) if axis not in (ap_axis, si_axis)]
    lateral_axis = int(lateral_axis[0]) if lateral_axis else 0

    return {
        "ap_axis": int(ap_axis),
        "si_axis": int(si_axis),
        "lateral_axis": lateral_axis,
        "orientation_confidence": float(confidence),
    }


def _reorient(mask: np.ndarray, ap_axis: int, si_axis: int) -> np.ndarray:
    lateral_axis = [axis for axis in range(3) if axis not in (ap_axis, si_axis)]
    if not lateral_axis:
        raise ValueError("unable to derive lateral axis")
    order = (ap_axis, si_axis, lateral_axis[0])
    return np.moveaxis(mask, order, (0, 1, 2))


def _infer_anterior_is_low_index(reoriented_mask: np.ndarray, edge_fraction: float) -> tuple[bool, float, str]:
    ap_len = int(reoriented_mask.shape[0])
    if ap_len <= 2:
        return True, 0.0, "insufficient_ap_samples"

    edge_width = max(1, int(round(ap_len * edge_fraction)))
    area_profile = reoriented_mask.sum(axis=(1, 2)).astype(np.float64)
    left_edge = float(np.median(area_profile[:edge_width]))
    right_edge = float(np.median(area_profile[-edge_width:]))

    if left_edge == 0.0 and right_edge == 0.0:
        return True, 0.0, "empty_edge_profile"

    if right_edge > left_edge:
        confidence = _clamp01(abs(right_edge - left_edge) / max(right_edge, left_edge, 1.0))
        return True, confidence, "posterior_likely_high_index"

    confidence = _clamp01(abs(left_edge - right_edge) / max(left_edge, right_edge, 1.0))
    return False, confidence, "posterior_likely_low_index"


def _window_median(values: np.ndarray, center_index: int, width: int) -> float:
    if values.size == 0:
        return float("nan")

    width = max(1, int(width))
    left = max(0, int(center_index) - width // 2)
    right = min(int(values.size), left + width)
    left = max(0, right - width)
    window = values[left:right]
    if window.size == 0:
        return float("nan")
    return float(np.median(window))


@dataclass(frozen=True)
class VertebralAxisInfo:
    ap_axis: int
    si_axis: int
    lateral_axis: int
    orientation_confidence: float


def isolate_vertebral_body(
    mask_data: ArrayLike,
    spacing_mm: Any = DEFAULT_SPACING_MM,
    core_radius_mm: float = DEFAULT_CORE_RADIUS_MM,
    restore_radius_mm: float = DEFAULT_RESTORE_RADIUS_MM,
    crop_padding_voxels: int = 6,
    ap_axis: int | None = None,
    si_axis: int | None = None,
) -> dict[str, Any]:
    """
    Derive a conservative body mask from a vertebral segmentation.

    The method keeps the largest connected component and uses a distance-based
    interior core to suppress thin posterior processes and other appendages.
    """

    mask = _as_bool_mask(mask_data)
    spacing = _normalize_spacing(spacing_mm)
    qc_flags: list[str] = []
    axis_info = _infer_axes(mask, ap_axis=ap_axis, si_axis=si_axis)

    original_voxels = _component_voxel_count(mask)
    if original_voxels == 0:
        qc_flags.append("empty_mask")
        return {
            "body_mask": mask.copy(),
            "original_voxels": 0,
            "body_voxels": 0,
            "body_fraction": 0.0,
            "axis_info": axis_info,
            "qc_flags": qc_flags,
        }

    mask = _largest_connected_component(mask)
    cropped_mask, crop_slices = _crop_to_mask_bbox(mask, padding_voxels=crop_padding_voxels)

    interior = cropped_mask.copy()
    thresholds = [
        float(core_radius_mm),
        max(float(core_radius_mm) * 0.75, min(spacing) * 0.5),
        max(float(core_radius_mm) * 0.5, min(spacing) * 0.5),
    ]
    for threshold_mm in thresholds:
        if threshold_mm <= 0:
            continue
        dist = distance_transform_edt(cropped_mask, sampling=spacing)
        interior = dist >= threshold_mm
        if np.any(interior):
            break

    if not np.any(interior):
        qc_flags.append("core_empty_after_erosion")
        interior = cropped_mask.copy()

    interior = _largest_connected_component(interior)

    restore_iters = max(1, int(round(float(restore_radius_mm) / max(min(spacing), 1e-6))))
    body_mask = binary_dilation(interior, structure=np.ones((3, 3, 3), dtype=np.uint8), iterations=restore_iters)
    body_mask = np.asarray(body_mask, dtype=bool) & cropped_mask

    if not np.any(body_mask):
        qc_flags.append("dilation_removed_body")
        body_mask = interior.copy()

    full_body_mask = _restore_from_crop(body_mask, mask.shape, crop_slices)
    reoriented_body_mask = _reorient(full_body_mask, axis_info["ap_axis"], axis_info["si_axis"])
    cropped_reoriented_body_mask = _crop_reoriented_to_dominant_ap_span(reoriented_body_mask)
    full_body_mask = np.moveaxis(
        cropped_reoriented_body_mask,
        (0, 1, 2),
        (axis_info["ap_axis"], axis_info["si_axis"], axis_info["lateral_axis"]),
    )

    body_voxels = _component_voxel_count(full_body_mask)
    body_fraction = 0.0 if original_voxels == 0 else float(body_voxels / original_voxels)

    if body_fraction < 0.35:
        qc_flags.append("body_fraction_low")

    if axis_info["orientation_confidence"] < 0.2:
        qc_flags.append("axis_orientation_ambiguous")

    return {
        "body_mask": full_body_mask,
        "original_voxels": original_voxels,
        "body_voxels": body_voxels,
        "body_fraction": body_fraction,
        "axis_info": axis_info,
        "qc_flags": qc_flags,
    }


def estimate_vertebral_heights(
    mask_data: ArrayLike,
    spacing_mm: Any = DEFAULT_SPACING_MM,
    body_mask: np.ndarray | None = None,
    ap_axis: int | None = None,
    si_axis: int | None = None,
    edge_fraction: float = DEFAULT_EDGE_FRACTION,
    smoothing_window: int = DEFAULT_SMOOTHING_WINDOW,
) -> dict[str, Any]:
    """
    Estimate anterior, middle, and posterior heights from a vertebral body.
    """

    mask = _as_bool_mask(mask_data)
    spacing = _normalize_spacing(spacing_mm)
    qc_flags: list[str] = []

    if body_mask is None:
        body_result = isolate_vertebral_body(mask, spacing_mm=spacing, ap_axis=ap_axis, si_axis=si_axis)
        body_mask = np.asarray(body_result["body_mask"], dtype=bool)
        qc_flags.extend(body_result.get("qc_flags", []))
        axis_info = body_result["axis_info"]
    else:
        body_mask = np.asarray(body_mask) > 0
        axis_info = _infer_axes(body_mask, ap_axis=ap_axis, si_axis=si_axis)

    if not np.any(body_mask):
        qc_flags.append("empty_body_mask")
        return {
            "ap_axis": axis_info["ap_axis"],
            "si_axis": axis_info["si_axis"],
            "lateral_axis": axis_info["lateral_axis"],
            "orientation_confidence": axis_info["orientation_confidence"],
            "ap_depth_mm": 0.0,
            "ap_positions": [],
            "height_profile_mm": [],
            "area_profile_voxels": [],
            "anterior_height_mm": None,
            "middle_height_mm": None,
            "posterior_height_mm": None,
            "anterior_area_voxels": None,
            "middle_area_voxels": None,
            "posterior_area_voxels": None,
            "anterior_posterior_ratio": None,
            "middle_posterior_ratio": None,
            "anterior_middle_ratio": None,
            "height_uniformity_ratio": None,
            "anterior_is_low_index": True,
            "orientation_status": "indeterminate",
            "qc_flags": qc_flags,
        }

    reoriented = _reorient(body_mask, axis_info["ap_axis"], axis_info["si_axis"])
    ap_len = int(reoriented.shape[0])
    ap_positions = np.where(reoriented.any(axis=(1, 2)))[0]
    if ap_positions.size < 3:
        qc_flags.append("insufficient_ap_samples")
    if ap_positions.size == 0:
        qc_flags.append("empty_ap_profile")
        return {
            "ap_axis": axis_info["ap_axis"],
            "si_axis": axis_info["si_axis"],
            "lateral_axis": axis_info["lateral_axis"],
            "orientation_confidence": 0.0,
            "orientation_note": "empty_ap_profile",
            "anterior_is_low_index": True,
            "orientation_status": "indeterminate",
            "ap_depth_mm": 0.0,
            "ap_positions": [],
            "height_profile_mm": [],
            "area_profile_voxels": [],
            "anterior_height_mm": None,
            "middle_height_mm": None,
            "posterior_height_mm": None,
            "anterior_area_voxels": None,
            "middle_area_voxels": None,
            "posterior_area_voxels": None,
            "anterior_posterior_ratio": None,
            "middle_posterior_ratio": None,
            "anterior_middle_ratio": None,
            "height_uniformity_ratio": None,
            "qc_flags": qc_flags,
        }

    ap_start = int(ap_positions[0])
    ap_stop = int(ap_positions[-1] + 1)
    reoriented_occupied = reoriented[ap_start:ap_stop]
    ap_len_occupied = int(reoriented_occupied.shape[0])

    height_profile_vox = np.zeros(ap_len_occupied, dtype=np.float64)
    area_profile_vox = reoriented_occupied.sum(axis=(1, 2)).astype(np.float64)
    for ap_idx in range(ap_len_occupied):
        slab = reoriented_occupied[ap_idx]
        occupied_si = np.where(slab.any(axis=1))[0]
        if occupied_si.size == 0:
            continue
        height_profile_vox[ap_idx] = float(occupied_si[-1] - occupied_si[0] + 1)

    height_profile_mm = height_profile_vox * float(spacing[axis_info["si_axis"]])
    height_profile_mm = _moving_average(height_profile_mm, smoothing_window)
    area_profile_vox = _moving_average(area_profile_vox, smoothing_window)

    anterior_is_low_index, orientation_confidence, orientation_note = _infer_anterior_is_low_index(
        reoriented_occupied,
        edge_fraction,
    )
    if orientation_confidence < 0.2:
        qc_flags.append("orientation_ambiguous")

    edge_width = max(1, int(round(ap_len_occupied * edge_fraction)))
    left_indices = np.arange(0, edge_width)
    right_indices = np.arange(max(0, ap_len_occupied - edge_width), ap_len_occupied)
    mid_start = max(0, ap_len_occupied // 2 - edge_width // 2)
    mid_end = min(ap_len_occupied, mid_start + edge_width)
    middle_indices = np.arange(mid_start, mid_end)

    if not anterior_is_low_index:
        anterior_indices = right_indices
        posterior_indices = left_indices
    else:
        anterior_indices = left_indices
        posterior_indices = right_indices

    if anterior_indices.size == 0 or posterior_indices.size == 0 or middle_indices.size == 0:
        qc_flags.append("insufficient_sampling_windows")

    anterior_height = _window_median(height_profile_mm, int(np.median(anterior_indices)) if anterior_indices.size else 0, edge_width)
    middle_height = _window_median(height_profile_mm, int(np.median(middle_indices)) if middle_indices.size else ap_len_occupied // 2, edge_width)
    posterior_height = _window_median(height_profile_mm, int(np.median(posterior_indices)) if posterior_indices.size else ap_len_occupied - 1, edge_width)
    anterior_area = _window_median(area_profile_vox, int(np.median(anterior_indices)) if anterior_indices.size else 0, edge_width)
    middle_area = _window_median(area_profile_vox, int(np.median(middle_indices)) if middle_indices.size else ap_len_occupied // 2, edge_width)
    posterior_area = _window_median(area_profile_vox, int(np.median(posterior_indices)) if posterior_indices.size else ap_len_occupied - 1, edge_width)

    ap_span_vox = ap_len_occupied
    ap_depth_mm = float(ap_span_vox * spacing[axis_info["ap_axis"]])

    heights = np.array([anterior_height, middle_height, posterior_height], dtype=np.float64)
    max_height = float(np.nanmax(heights)) if np.any(np.isfinite(heights)) else float("nan")
    min_height = float(np.nanmin(heights)) if np.any(np.isfinite(heights)) else float("nan")

    if np.isfinite(max_height) and max_height > 0:
        anterior_posterior_ratio = float(anterior_height / posterior_height) if posterior_height and posterior_height > 0 else None
        middle_posterior_ratio = float(middle_height / posterior_height) if posterior_height and posterior_height > 0 else None
        anterior_middle_ratio = float(anterior_height / middle_height) if middle_height and middle_height > 0 else None
        height_uniformity_ratio = float(min_height / max_height) if max_height > 0 else None
    else:
        anterior_posterior_ratio = None
        middle_posterior_ratio = None
        anterior_middle_ratio = None
        height_uniformity_ratio = None

    if ap_depth_mm > 0 and max_height > 0 and (max_height / ap_depth_mm) < 0.45:
        qc_flags.append("height_depth_ratio_low")

    return {
        "ap_axis": axis_info["ap_axis"],
        "si_axis": axis_info["si_axis"],
        "lateral_axis": axis_info["lateral_axis"],
        "orientation_confidence": float(orientation_confidence),
        "orientation_note": orientation_note,
        "anterior_is_low_index": bool(anterior_is_low_index),
        "orientation_status": "known" if orientation_confidence >= 0.2 else "indeterminate",
        "ap_depth_mm": ap_depth_mm,
        "ap_positions": ap_positions.tolist(),
        "height_profile_mm": height_profile_mm.tolist(),
        "area_profile_voxels": area_profile_vox.tolist(),
        "anterior_height_mm": float(anterior_height) if np.isfinite(anterior_height) else None,
        "middle_height_mm": float(middle_height) if np.isfinite(middle_height) else None,
        "posterior_height_mm": float(posterior_height) if np.isfinite(posterior_height) else None,
        "anterior_area_voxels": float(anterior_area) if np.isfinite(anterior_area) else None,
        "middle_area_voxels": float(middle_area) if np.isfinite(middle_area) else None,
        "posterior_area_voxels": float(posterior_area) if np.isfinite(posterior_area) else None,
        "anterior_posterior_ratio": anterior_posterior_ratio,
        "middle_posterior_ratio": middle_posterior_ratio,
        "anterior_middle_ratio": anterior_middle_ratio,
        "height_uniformity_ratio": height_uniformity_ratio,
        "qc_flags": qc_flags,
    }


def classify_fracture_pattern(
    height_result: dict[str, Any],
    wedge_threshold: float = 0.80,
    biconcave_threshold: float = 0.80,
    crush_depth_ratio_threshold: float = 0.55,
) -> dict[str, Any]:
    """
    Convert vertebral morphometry into a Genant semi-quantitative grade.
    """

    qc_flags = list(height_result.get("qc_flags", []))
    anterior = height_result.get("anterior_height_mm")
    middle = height_result.get("middle_height_mm")
    posterior = height_result.get("posterior_height_mm")
    anterior_area = height_result.get("anterior_area_voxels")
    middle_area = height_result.get("middle_area_voxels")
    posterior_area = height_result.get("posterior_area_voxels")
    ap_depth_mm = height_result.get("ap_depth_mm")
    orientation_confidence = float(height_result.get("orientation_confidence") or 0.0)
    severity_by_grade = {0: "none", 1: "mild", 2: "moderate", 3: "severe"}

    if any(value is None for value in (anterior, middle, posterior)):
        qc_flags.append("missing_height_measurement")
        return {
            "screen_status": "indeterminate",
            "screen_label": "indeterminate",
            "screen_confidence": 0.0,
            "genant_grade": None,
            "genant_label": "indeterminate",
            "severity": "indeterminate",
            "suspected_pattern": None,
            "ratios": {
                "anterior_posterior_ratio": height_result.get("anterior_posterior_ratio"),
                "middle_posterior_ratio": height_result.get("middle_posterior_ratio"),
                "anterior_middle_ratio": height_result.get("anterior_middle_ratio"),
                "height_uniformity_ratio": height_result.get("height_uniformity_ratio"),
                "height_loss_ratio_percent": None,
                "posterior_height_loss_ratio_percent": None,
                "area_loss_ratio_percent": None,
            },
            "qc_flags": qc_flags,
        }

    anterior = float(anterior)
    middle = float(middle)
    posterior = float(posterior)
    ap_depth_mm = float(ap_depth_mm or 0.0)
    area_values = [
        float(value)
        for value in (anterior_area, middle_area, posterior_area)
        if value is not None
    ]
    height_values = [anterior, middle, posterior]
    lowest_height = float(min(height_values))
    highest_height = float(max(height_values))
    reference_height = float(max(highest_height, posterior))
    posterior_height_loss = float((1.0 - (lowest_height / posterior)) * 100.0) if posterior > 0 else None
    height_loss_percent = float((1.0 - (lowest_height / reference_height)) * 100.0) if reference_height > 0 else None
    height_depth_ratio = float(highest_height / ap_depth_mm) if ap_depth_mm > 0 else None

    lowest_area = float(min(area_values)) if area_values else None
    reference_area = float(max(area_values)) if area_values else None
    area_loss_percent = (
        float((1.0 - (lowest_area / reference_area)) * 100.0)
        if lowest_area is not None and reference_area is not None and reference_area > 0
        else None
    )

    ratios = {
        "anterior_posterior_ratio": float(anterior / posterior) if posterior > 0 else None,
        "middle_posterior_ratio": float(middle / posterior) if posterior > 0 else None,
        "anterior_middle_ratio": float(anterior / middle) if middle > 0 else None,
        "height_uniformity_ratio": float(min(anterior, middle, posterior) / max(anterior, middle, posterior)) if max(anterior, middle, posterior) > 0 else None,
        "lowest_height_mm": lowest_height,
        "reference_height_mm": reference_height,
        "posterior_height_reference_mm": posterior,
        "height_loss_ratio_percent": height_loss_percent,
        "posterior_height_loss_ratio_percent": posterior_height_loss,
        "height_depth_ratio": height_depth_ratio,
        "lowest_area_voxels": lowest_area,
        "reference_area_voxels": reference_area,
        "area_loss_ratio_percent": area_loss_percent,
    }

    label = "indeterminate"
    genant_grade = None
    severity = "indeterminate"
    suspected_pattern = None
    confidence = 0.0

    if height_loss_percent is not None:
        if height_loss_percent > 40.0:
            genant_grade = 3
        elif height_loss_percent >= 25.0:
            genant_grade = 2
        elif height_loss_percent >= 20.0:
            genant_grade = 1
        else:
            genant_grade = 0

        label = f"grade_{genant_grade}"
        severity = severity_by_grade[genant_grade]
        if genant_grade >= 1:
            confidence = _clamp01(height_loss_percent / 100.0)

        if genant_grade >= 1:
            if (
                ratios["anterior_posterior_ratio"] is not None
                and ratios["middle_posterior_ratio"] is not None
                and ratios["anterior_posterior_ratio"] <= wedge_threshold
                and ratios["middle_posterior_ratio"] >= min(0.98, wedge_threshold + 0.05)
            ):
                suspected_pattern = "wedge"
            elif (
                min(anterior, posterior) > 0
                and middle <= biconcave_threshold * min(anterior, posterior)
                and abs(anterior - posterior) / max(anterior, posterior) <= 0.15
            ):
                suspected_pattern = "biconcave"
            elif height_depth_ratio is not None and height_depth_ratio <= crush_depth_ratio_threshold:
                suspected_pattern = "crush"
            else:
                lowest_height_name = min(
                    (
                        ("anterior", anterior),
                        ("middle", middle),
                        ("posterior", posterior),
                    ),
                    key=lambda item: item[1],
                )[0]
                if lowest_height_name == "anterior":
                    suspected_pattern = "wedge"
                elif lowest_height_name == "middle":
                    suspected_pattern = "biconcave"
                elif height_depth_ratio is not None and height_depth_ratio <= (crush_depth_ratio_threshold + 0.10):
                    suspected_pattern = "crush"

    if orientation_confidence < 0.2:
        qc_flags.append("orientation_ambiguous")
        confidence = min(confidence, 0.65)

    if genant_grade is None:
        if qc_flags:
            qc_flags.append("genant_grade_unavailable")
            return {
                "screen_status": "indeterminate",
                "screen_label": "indeterminate",
                "screen_confidence": 0.0,
                "genant_grade": None,
                "genant_label": "indeterminate",
                "severity": "indeterminate",
                "suspected_pattern": None,
                "ratios": ratios,
                "qc_flags": qc_flags,
            }
        qc_flags.append("genant_grade_unavailable")
        return {
            "screen_status": "indeterminate",
            "screen_label": "indeterminate",
            "screen_confidence": 0.0,
            "genant_grade": None,
            "genant_label": "indeterminate",
            "severity": "indeterminate",
            "suspected_pattern": None,
            "ratios": ratios,
            "qc_flags": qc_flags,
        }

    if genant_grade == 0:
        qc_flags.append("no_qualifying_genant_deformity")

    return {
        "screen_status": "suspected" if genant_grade >= 1 else "no_suspicion",
        "screen_label": label,
        "screen_confidence": float(confidence),
        "genant_grade": genant_grade,
        "genant_label": label,
        "severity": severity,
        "suspected_pattern": suspected_pattern,
        "ratios": ratios,
        "qc_flags": qc_flags,
    }


def _severity_from_genant_grade(genant_grade: int | None) -> str:
    if genant_grade is None:
        return "indeterminate"
    if genant_grade <= 0:
        return "none"
    if genant_grade == 1:
        return "mild"
    if genant_grade == 2:
        return "moderate"
    return "severe"


def _select_height_key_from_summary(summary: dict[str, Any]) -> str | None:
    morphometry = summary.get("morphometry", {}) if isinstance(summary.get("morphometry"), dict) else {}
    components = {
        "anterior_height_mm": morphometry.get("anterior_height_mm"),
        "middle_height_mm": morphometry.get("middle_height_mm"),
        "posterior_height_mm": morphometry.get("posterior_height_mm"),
    }
    pattern = str(summary.get("suspected_pattern") or "").strip().lower()
    if pattern == "wedge" and components["anterior_height_mm"] is not None:
        return "anterior_height_mm"
    if pattern == "biconcave" and components["middle_height_mm"] is not None:
        return "middle_height_mm"

    finite_components = [(key, float(value)) for key, value in components.items() if value is not None]
    if not finite_components:
        return None
    return min(finite_components, key=lambda item: item[1])[0]


def _nearest_adjacent_levels(level: str, available_levels: list[str]) -> list[str]:
    target_index = vertebra_level_index(level)
    if target_index is None:
        return []

    indexed_levels = sorted(
        ((candidate, vertebra_level_index(candidate)) for candidate in available_levels),
        key=lambda item: (-1 if item[1] is None else item[1]),
    )
    lower: str | None = None
    upper: str | None = None
    for candidate, candidate_index in indexed_levels:
        if candidate_index is None or candidate == level:
            continue
        if candidate_index < target_index:
            lower = candidate
        elif candidate_index > target_index and upper is None:
            upper = candidate
            break
    adjacent = []
    if lower is not None:
        adjacent.append(lower)
    if upper is not None:
        adjacent.append(upper)
    return adjacent


def refine_classification_with_adjacent_reference(
    per_vertebra: dict[str, dict[str, Any]],
    *,
    pathologic_threshold_percent: float = 20.0,
) -> dict[str, dict[str, Any]]:
    """
    Reclassify vertebrae using adjacent vertebral heights as the primary reference.

    The per-vertebra summaries are expected to contain:
    - `morphometry.anterior_height_mm`, `middle_height_mm`, `posterior_height_mm`
    - provisional `screen_label` / `genant_grade`
    - optional `suspected_pattern`
    """

    available_levels = [
        level
        for level, summary in per_vertebra.items()
        if isinstance(summary, dict) and isinstance(summary.get("morphometry"), dict)
    ]
    refined: dict[str, dict[str, Any]] = {}

    for level, summary in per_vertebra.items():
        if not isinstance(summary, dict):
            refined[level] = summary
            continue

        morphometry = summary.get("morphometry", {}) if isinstance(summary.get("morphometry"), dict) else {}
        qc_flags = list(summary.get("qc_flags", []))
        ratios = dict(summary.get("ratios", {})) if isinstance(summary.get("ratios"), dict) else {}
        height_key = _select_height_key_from_summary(summary)
        if height_key is None:
            qc_flags.append("adjacent_reference_target_height_unavailable")
            updated = dict(summary)
            updated["qc_flags"] = sorted(set(qc_flags))
            refined[level] = updated
            continue

        target_height = morphometry.get(height_key)
        if target_height is None:
            qc_flags.append("adjacent_reference_target_height_unavailable")
            updated = dict(summary)
            updated["qc_flags"] = sorted(set(qc_flags))
            refined[level] = updated
            continue

        adjacent_levels = _nearest_adjacent_levels(level, available_levels)
        normal_reference_levels = []
        fallback_reference_levels = []
        for candidate in adjacent_levels:
            candidate_summary = per_vertebra.get(candidate, {})
            candidate_morphometry = (
                candidate_summary.get("morphometry", {})
                if isinstance(candidate_summary.get("morphometry"), dict)
                else {}
            )
            candidate_height = candidate_morphometry.get(height_key)
            if candidate_height is None:
                continue
            if candidate_summary.get("genant_grade") == 0:
                normal_reference_levels.append(candidate)
            else:
                fallback_reference_levels.append(candidate)

        reference_levels = normal_reference_levels or fallback_reference_levels
        reference_heights = []
        for candidate in reference_levels:
            candidate_summary = per_vertebra.get(candidate, {})
            candidate_morphometry = (
                candidate_summary.get("morphometry", {})
                if isinstance(candidate_summary.get("morphometry"), dict)
                else {}
            )
            candidate_height = candidate_morphometry.get(height_key)
            if candidate_height is not None:
                reference_heights.append(float(candidate_height))

        updated = dict(summary)
        if "height_loss_ratio_percent" in ratios:
            ratios["intra_vertebral_height_loss_ratio_percent"] = ratios.get("height_loss_ratio_percent")

        ratios["reference_height_key"] = height_key
        ratios["adjacent_reference_levels"] = reference_levels

        if not reference_heights:
            qc_flags.append("adjacent_reference_unavailable")
            updated["status"] = "indeterminate"
            updated["screen_label"] = "indeterminate"
            updated["genant_label"] = "indeterminate"
            updated["genant_grade"] = None
            updated["severity"] = "indeterminate"
            updated["screen_confidence"] = 0.0
            ratios["adjacent_reference_height_mm"] = None
            ratios["adjacent_target_height_mm"] = float(target_height)
            ratios["height_loss_ratio_percent"] = None
            updated["ratios"] = ratios
            updated["qc_flags"] = sorted(set(qc_flags))
            refined[level] = updated
            continue

        if not normal_reference_levels:
            qc_flags.append("adjacent_reference_not_normal")

        reference_height = float(np.median(np.asarray(reference_heights, dtype=np.float64)))
        height_loss_percent = (
            float((1.0 - (float(target_height) / reference_height)) * 100.0)
            if reference_height > 0
            else None
        )
        ratios["adjacent_reference_height_mm"] = reference_height
        ratios["adjacent_target_height_mm"] = float(target_height)
        ratios["height_loss_ratio_percent"] = height_loss_percent

        if height_loss_percent is None:
            qc_flags.append("adjacent_reference_invalid")
            updated["status"] = "indeterminate"
            updated["screen_label"] = "indeterminate"
            updated["genant_label"] = "indeterminate"
            updated["genant_grade"] = None
            updated["severity"] = "indeterminate"
            updated["screen_confidence"] = 0.0
            updated["ratios"] = ratios
            updated["qc_flags"] = sorted(set(qc_flags))
            refined[level] = updated
            continue

        if height_loss_percent > 40.0:
            genant_grade = 3
        elif height_loss_percent >= 25.0:
            genant_grade = 2
        elif height_loss_percent >= pathologic_threshold_percent:
            genant_grade = 1
        else:
            genant_grade = 0

        updated["status"] = "suspected" if genant_grade >= 1 else "no_suspicion"
        updated["screen_label"] = f"grade_{genant_grade}"
        updated["genant_label"] = f"grade_{genant_grade}"
        updated["genant_grade"] = genant_grade
        updated["severity"] = _severity_from_genant_grade(genant_grade)
        updated["screen_confidence"] = _clamp01((height_loss_percent / 100.0) + (0.1 * max(0, len(normal_reference_levels) - 1)))
        updated["ratios"] = ratios
        updated["qc_flags"] = sorted(set(qc_flags))
        refined[level] = updated

    return refined


def screen_vertebral_fracture(
    mask_data: ArrayLike,
    spacing_mm: Any = DEFAULT_SPACING_MM,
    ap_axis: int | None = None,
    si_axis: int | None = None,
    core_radius_mm: float = DEFAULT_CORE_RADIUS_MM,
    restore_radius_mm: float = DEFAULT_RESTORE_RADIUS_MM,
    edge_fraction: float = DEFAULT_EDGE_FRACTION,
    smoothing_window: int = DEFAULT_SMOOTHING_WINDOW,
) -> dict[str, Any]:
    """
    Run the full heuristic vertebral fracture triage pipeline.
    """

    body_result = isolate_vertebral_body(
        mask_data,
        spacing_mm=spacing_mm,
        core_radius_mm=core_radius_mm,
        restore_radius_mm=restore_radius_mm,
        ap_axis=ap_axis,
        si_axis=si_axis,
    )
    height_result = estimate_vertebral_heights(
        mask_data,
        spacing_mm=spacing_mm,
        body_mask=body_result["body_mask"],
        ap_axis=ap_axis,
        si_axis=si_axis,
        edge_fraction=edge_fraction,
        smoothing_window=smoothing_window,
    )
    classification = classify_fracture_pattern(height_result)

    qc_flags = []
    qc_flags.extend(body_result.get("qc_flags", []))
    qc_flags.extend(height_result.get("qc_flags", []))
    qc_flags.extend(classification.get("qc_flags", []))

    return {
        "job_name": "vertebral_fracture_screen",
        "status": classification["screen_status"],
        "screen_label": classification["screen_label"],
        "screen_confidence": classification["screen_confidence"],
        "genant_grade": classification["genant_grade"],
        "genant_label": classification["genant_label"],
        "severity": classification["severity"],
        "suspected_pattern": classification["suspected_pattern"],
        "body_isolation": {
            "original_voxels": body_result["original_voxels"],
            "body_voxels": body_result["body_voxels"],
            "body_fraction": body_result["body_fraction"],
            "axis_info": body_result["axis_info"],
        },
        "morphometry": {
            "ap_axis": height_result["ap_axis"],
            "si_axis": height_result["si_axis"],
            "lateral_axis": height_result["lateral_axis"],
            "orientation_confidence": height_result["orientation_confidence"],
            "anterior_is_low_index": height_result["anterior_is_low_index"],
            "ap_depth_mm": height_result["ap_depth_mm"],
            "anterior_height_mm": height_result["anterior_height_mm"],
            "middle_height_mm": height_result["middle_height_mm"],
            "posterior_height_mm": height_result["posterior_height_mm"],
            "anterior_area_voxels": height_result["anterior_area_voxels"],
            "middle_area_voxels": height_result["middle_area_voxels"],
            "posterior_area_voxels": height_result["posterior_area_voxels"],
            "height_profile_mm": height_result["height_profile_mm"],
            "area_profile_voxels": height_result["area_profile_voxels"],
        },
        "ratios": classification["ratios"],
        "qc_flags": sorted(set(qc_flags)),
    }
