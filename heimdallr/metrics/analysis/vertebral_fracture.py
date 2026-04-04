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

    interior = mask.copy()
    thresholds = [
        float(core_radius_mm),
        max(float(core_radius_mm) * 0.75, min(spacing) * 0.5),
        max(float(core_radius_mm) * 0.5, min(spacing) * 0.5),
    ]
    for threshold_mm in thresholds:
        if threshold_mm <= 0:
            continue
        dist = distance_transform_edt(mask, sampling=spacing)
        interior = dist >= threshold_mm
        if np.any(interior):
            break

    if not np.any(interior):
        qc_flags.append("core_empty_after_erosion")
        interior = mask.copy()

    interior = _largest_connected_component(interior)

    restore_iters = max(1, int(round(float(restore_radius_mm) / max(min(spacing), 1e-6))))
    body_mask = binary_dilation(interior, structure=np.ones((3, 3, 3), dtype=np.uint8), iterations=restore_iters)
    body_mask = np.asarray(body_mask, dtype=bool) & mask

    if not np.any(body_mask):
        qc_flags.append("dilation_removed_body")
        body_mask = interior.copy()

    body_voxels = _component_voxel_count(body_mask)
    body_fraction = 0.0 if original_voxels == 0 else float(body_voxels / original_voxels)

    if body_fraction < 0.35:
        qc_flags.append("body_fraction_low")

    if axis_info["orientation_confidence"] < 0.2:
        qc_flags.append("axis_orientation_ambiguous")

    return {
        "body_mask": body_mask,
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
    si_len = int(reoriented.shape[1])

    ap_positions = np.where(reoriented.any(axis=(1, 2)))[0]
    if ap_positions.size < 3:
        qc_flags.append("insufficient_ap_samples")

    height_profile_vox = np.zeros(ap_len, dtype=np.float64)
    area_profile_vox = reoriented.sum(axis=(1, 2)).astype(np.float64)
    for ap_idx in range(ap_len):
        slab = reoriented[ap_idx]
        occupied_si = np.where(slab.any(axis=1))[0]
        if occupied_si.size == 0:
            continue
        height_profile_vox[ap_idx] = float(occupied_si[-1] - occupied_si[0] + 1)

    height_profile_mm = height_profile_vox * float(spacing[axis_info["si_axis"]])
    height_profile_mm = _moving_average(height_profile_mm, smoothing_window)
    area_profile_vox = _moving_average(area_profile_vox, smoothing_window)

    anterior_is_low_index, orientation_confidence, orientation_note = _infer_anterior_is_low_index(reoriented, edge_fraction)
    if orientation_confidence < 0.2:
        qc_flags.append("orientation_ambiguous")

    edge_width = max(1, int(round(ap_len * edge_fraction)))
    left_indices = np.arange(0, edge_width)
    right_indices = np.arange(max(0, ap_len - edge_width), ap_len)
    mid_start = max(0, ap_len // 2 - edge_width // 2)
    mid_end = min(ap_len, mid_start + edge_width)
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
    middle_height = _window_median(height_profile_mm, int(np.median(middle_indices)) if middle_indices.size else ap_len // 2, edge_width)
    posterior_height = _window_median(height_profile_mm, int(np.median(posterior_indices)) if posterior_indices.size else ap_len - 1, edge_width)

    ap_indices = np.where(reoriented.any(axis=(1, 2)))[0]
    if ap_indices.size > 0:
        ap_span_vox = int(ap_indices[-1] - ap_indices[0] + 1)
    else:
        ap_span_vox = 0
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
    Convert vertebral morphometry into a conservative triage label.

    The output uses neutral language:
    - suspected_wedge
    - suspected_biconcave
    - suspected_crush
    - indeterminate
    """

    qc_flags = list(height_result.get("qc_flags", []))
    anterior = height_result.get("anterior_height_mm")
    middle = height_result.get("middle_height_mm")
    posterior = height_result.get("posterior_height_mm")
    ap_depth_mm = height_result.get("ap_depth_mm")
    orientation_confidence = float(height_result.get("orientation_confidence") or 0.0)

    if any(value is None for value in (anterior, middle, posterior)):
        qc_flags.append("missing_height_measurement")
        return {
            "screen_status": "indeterminate",
            "screen_label": "indeterminate",
            "screen_confidence": 0.0,
            "suspected_pattern": None,
            "ratios": {
                "anterior_posterior_ratio": height_result.get("anterior_posterior_ratio"),
                "middle_posterior_ratio": height_result.get("middle_posterior_ratio"),
                "anterior_middle_ratio": height_result.get("anterior_middle_ratio"),
                "height_uniformity_ratio": height_result.get("height_uniformity_ratio"),
            },
            "qc_flags": qc_flags,
        }

    anterior = float(anterior)
    middle = float(middle)
    posterior = float(posterior)
    ap_depth_mm = float(ap_depth_mm or 0.0)

    ratios = {
        "anterior_posterior_ratio": float(anterior / posterior) if posterior > 0 else None,
        "middle_posterior_ratio": float(middle / posterior) if posterior > 0 else None,
        "anterior_middle_ratio": float(anterior / middle) if middle > 0 else None,
        "height_uniformity_ratio": float(min(anterior, middle, posterior) / max(anterior, middle, posterior)) if max(anterior, middle, posterior) > 0 else None,
    }

    label = "indeterminate"
    suspected_pattern = None
    confidence = 0.0

    if ratios["anterior_posterior_ratio"] is not None and ratios["middle_posterior_ratio"] is not None:
        if ratios["anterior_posterior_ratio"] <= wedge_threshold and ratios["middle_posterior_ratio"] >= wedge_threshold:
            label = "suspected_wedge"
            suspected_pattern = "wedge"
            confidence = _clamp01((wedge_threshold - ratios["anterior_posterior_ratio"]) / max(wedge_threshold, 1e-6))
            confidence = max(confidence, _clamp01((ratios["middle_posterior_ratio"] - wedge_threshold) / max(1.0 - wedge_threshold, 1e-6)))
        elif (
            min(anterior, posterior) > 0
            and middle <= biconcave_threshold * min(anterior, posterior)
            and abs(anterior - posterior) / max(anterior, posterior) <= 0.15
        ):
            label = "suspected_biconcave"
            suspected_pattern = "biconcave"
            confidence = _clamp01((biconcave_threshold * min(anterior, posterior) - middle) / max(max(anterior, posterior), 1.0))
        elif ap_depth_mm > 0 and (max(anterior, middle, posterior) / ap_depth_mm) <= crush_depth_ratio_threshold:
            label = "suspected_crush"
            suspected_pattern = "crush"
            confidence = _clamp01((crush_depth_ratio_threshold - (max(anterior, middle, posterior) / ap_depth_mm)) / crush_depth_ratio_threshold)

    if orientation_confidence < 0.2:
        qc_flags.append("orientation_ambiguous")
        confidence = min(confidence, 0.65)

    if label == "indeterminate" and not qc_flags:
        qc_flags.append("no_qualifying_fracture_pattern")

    return {
        "screen_status": "suspected" if suspected_pattern else "indeterminate",
        "screen_label": label,
        "screen_confidence": float(confidence),
        "suspected_pattern": suspected_pattern,
        "ratios": ratios,
        "qc_flags": qc_flags,
    }


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
            "height_profile_mm": height_result["height_profile_mm"],
            "area_profile_voxels": height_result["area_profile_voxels"],
        },
        "ratios": classification["ratios"],
        "qc_flags": sorted(set(qc_flags)),
    }
