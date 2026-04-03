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

"""Helpers for opportunistic abdominal body-fat metrics."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np

__all__ = [
    "ABDOMINAL_VERTEBRA_LEVELS",
    "compute_axial_mask_extent",
    "build_midpoint_slabs_from_centers",
    "build_abdominal_slabs",
    "calculate_body_fat_distribution",
    "compute_l3_slice_fat_areas",
]


ABDOMINAL_VERTEBRA_LEVELS = ("T12", "L1", "L2", "L3", "L4", "L5")


def _as_bool_mask(mask: np.ndarray) -> np.ndarray:
    return np.asarray(mask, dtype=bool)


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_spacing_mm(spacing_mm: Sequence[float] | None, ndim: int = 3) -> tuple[float, ...]:
    if spacing_mm is None:
        return tuple(1.0 for _ in range(ndim))

    parsed = []
    for item in spacing_mm:
        value = _safe_float(item)
        if value is not None:
            parsed.append(value)

    if not parsed:
        return tuple(1.0 for _ in range(ndim))
    if len(parsed) >= ndim:
        return tuple(parsed[:ndim])
    while len(parsed) < ndim:
        parsed.append(parsed[-1])
    return tuple(parsed)


def compute_axial_mask_extent(mask: np.ndarray) -> tuple[int, int] | None:
    """Return the first and last occupied axial slice for a mask."""
    mask_bool = _as_bool_mask(mask)
    if mask_bool.ndim != 3 or not np.any(mask_bool):
        return None

    z_indices = np.where(mask_bool.sum(axis=(0, 1)) > 0)[0]
    if z_indices.size == 0:
        return None
    return int(z_indices[0]), int(z_indices[-1])


def build_midpoint_slabs_from_centers(
    centers: Mapping[str, float],
    *,
    z_size: int,
) -> dict[str, dict[str, int | float | str]]:
    """Build contiguous z-slabs by splitting halfway between vertebral centers."""
    ordered = [
        (level, float(centers[level]))
        for level in ABDOMINAL_VERTEBRA_LEVELS
        if level in centers and centers[level] is not None
    ]
    if not ordered:
        return {}

    slabs: dict[str, dict[str, int | float | str]] = {}
    for index, (level, center) in enumerate(ordered):
        prev_center = ordered[index - 1][1] if index > 0 else None
        next_center = ordered[index + 1][1] if index < (len(ordered) - 1) else None

        if prev_center is None and next_center is None:
            start = max(0, int(round(center)))
            end = min(z_size - 1, int(round(center)))
        else:
            if prev_center is None:
                half_gap = (next_center - center) / 2.0
                start = max(0, int(np.floor(center - half_gap)))
            else:
                start = max(0, int(np.floor((prev_center + center) / 2.0)))

            if next_center is None:
                half_gap = (center - prev_center) / 2.0
                end = min(z_size - 1, int(np.ceil(center + half_gap)))
            else:
                end = min(z_size - 1, int(np.ceil((center + next_center) / 2.0)) - 1)

        if end < start:
            end = start

        slabs[level] = {
            "level": level,
            "start_slice": int(start),
            "end_slice": int(end),
            "center_slice": float(center),
            "strategy": "centroid_midpoint",
        }
    return slabs


def build_abdominal_slabs(
    vertebra_masks: Mapping[str, np.ndarray],
    *,
    z_size: int,
) -> dict[str, Any]:
    """Build vertebra-anchored abdominal slabs for T12-L5."""
    available_extents: dict[str, tuple[int, int]] = {}
    centers: dict[str, float] = {}
    for level in ABDOMINAL_VERTEBRA_LEVELS:
        mask = vertebra_masks.get(level)
        if mask is None:
            continue
        extent = compute_axial_mask_extent(mask)
        if extent is None:
            continue
        available_extents[level] = extent
        centers[level] = (extent[0] + extent[1]) / 2.0

    missing_levels = [level for level in ABDOMINAL_VERTEBRA_LEVELS if level not in available_extents]
    coverage_complete = not missing_levels

    if coverage_complete:
        slabs = build_midpoint_slabs_from_centers(centers, z_size=z_size)
        strategy = "centroid_midpoint"
    else:
        slabs = {}
        for level, (start, end) in available_extents.items():
            slabs[level] = {
                "level": level,
                "start_slice": int(start),
                "end_slice": int(end),
                "center_slice": float((start + end) / 2.0),
                "strategy": "mask_extent_fallback",
            }
        strategy = "mask_extent_fallback"

    available_levels = [level for level in ABDOMINAL_VERTEBRA_LEVELS if level in slabs]
    overall_start = min((slabs[level]["start_slice"] for level in available_levels), default=None)
    overall_end = max((slabs[level]["end_slice"] for level in available_levels), default=None)

    return {
        "strategy": strategy,
        "coverage_complete": coverage_complete,
        "available_levels": available_levels,
        "missing_levels": missing_levels,
        "slabs": slabs,
        "overall_start_slice": overall_start,
        "overall_end_slice": overall_end,
    }


def _voxel_volume_cm3(spacing_mm: Sequence[float] | None) -> float:
    sx, sy, sz = normalize_spacing_mm(spacing_mm, ndim=3)
    return (sx * sy * sz) / 1000.0


def _pixel_area_cm2(spacing_mm: Sequence[float] | None) -> float:
    sx, sy = normalize_spacing_mm(spacing_mm, ndim=2)
    return (sx * sy) / 100.0


def _volume_for_slices(mask: np.ndarray, start: int, end: int, spacing_mm: Sequence[float] | None) -> tuple[int, float]:
    mask_bool = _as_bool_mask(mask)
    if mask_bool.ndim != 3:
        return 0, 0.0
    clipped = mask_bool[:, :, start : end + 1]
    voxel_count = int(clipped.sum())
    return voxel_count, round(voxel_count * _voxel_volume_cm3(spacing_mm), 3)


def _ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return round(float(numerator) / float(denominator), 4)


def calculate_body_fat_distribution(
    *,
    subcutaneous_fat_mask: np.ndarray,
    torso_fat_mask: np.ndarray,
    spacing_mm: Sequence[float] | None,
    slab_definition: Mapping[str, Any],
) -> dict[str, Any]:
    """Calculate per-slab and aggregate abdominal fat volumes."""
    sat_mask = _as_bool_mask(subcutaneous_fat_mask)
    torso_mask = _as_bool_mask(torso_fat_mask)

    if sat_mask.shape != torso_mask.shape:
        raise ValueError(f"SAT and torso fat masks must match. Got {sat_mask.shape} vs {torso_mask.shape}")

    slab_results: dict[str, dict[str, Any]] = {}
    total_sat_cm3 = 0.0
    total_torso_cm3 = 0.0
    total_slices = 0

    slabs = slab_definition.get("slabs") or {}
    for level in ABDOMINAL_VERTEBRA_LEVELS:
        slab = slabs.get(level)
        if not slab:
            continue

        start = int(slab["start_slice"])
        end = int(slab["end_slice"])
        sat_voxels, sat_cm3 = _volume_for_slices(sat_mask, start, end, spacing_mm)
        torso_voxels, torso_cm3 = _volume_for_slices(torso_mask, start, end, spacing_mm)
        n_slices = int(end - start + 1)
        total_sat_cm3 += sat_cm3
        total_torso_cm3 += torso_cm3
        total_slices += n_slices

        slab_results[level] = {
            "level": level,
            "start_slice": start,
            "end_slice": end,
            "slice_count": n_slices,
            "strategy": slab.get("strategy"),
            "subcutaneous_fat_voxels": sat_voxels,
            "subcutaneous_fat_cm3": sat_cm3,
            "torso_fat_voxels": torso_voxels,
            "torso_fat_cm3": torso_cm3,
            "torso_to_subcutaneous_ratio": _ratio(torso_cm3, sat_cm3),
        }

    aggregate = {
        "abdominal_levels": [level for level in ABDOMINAL_VERTEBRA_LEVELS if level in slab_results],
        "coverage_complete": bool(slab_definition.get("coverage_complete")),
        "missing_levels": list(slab_definition.get("missing_levels") or []),
        "overall_start_slice": slab_definition.get("overall_start_slice"),
        "overall_end_slice": slab_definition.get("overall_end_slice"),
        "slice_count": total_slices,
        "slab_strategy": slab_definition.get("strategy"),
        "subcutaneous_fat_cm3": round(total_sat_cm3, 3),
        "torso_fat_cm3": round(total_torso_cm3, 3),
        "torso_to_subcutaneous_ratio": _ratio(total_torso_cm3, total_sat_cm3),
    }
    aggregate["needs_manual_review"] = bool(not aggregate["coverage_complete"] or not slab_results)

    return {
        "slabs": slab_results,
        "aggregate": aggregate,
    }


def compute_l3_slice_fat_areas(
    *,
    vertebra_l3_mask: np.ndarray,
    subcutaneous_fat_mask: np.ndarray,
    torso_fat_mask: np.ndarray,
    spacing_mm: Sequence[float] | None,
) -> dict[str, Any]:
    """Compute SAT and torso fat areas at the midpoint slice of L3."""
    l3_mask = _as_bool_mask(vertebra_l3_mask)
    sat_mask = _as_bool_mask(subcutaneous_fat_mask)
    torso_mask = _as_bool_mask(torso_fat_mask)

    if l3_mask.shape != sat_mask.shape or l3_mask.shape != torso_mask.shape:
        raise ValueError("L3, SAT, and torso fat masks must share the same shape")

    extent = compute_axial_mask_extent(l3_mask)
    if extent is None:
        return {
            "status": "missing",
            "slice_index": None,
            "l3_slice_count": 0,
            "subcutaneous_fat_area_cm2": None,
            "torso_fat_area_cm2": None,
            "torso_to_subcutaneous_ratio": None,
        }

    start, end = extent
    slice_index = int(round((start + end) / 2.0))
    pixel_area_cm2 = _pixel_area_cm2(spacing_mm)
    sat_pixels = int(sat_mask[:, :, slice_index].sum())
    torso_pixels = int(torso_mask[:, :, slice_index].sum())
    sat_cm2 = round(sat_pixels * pixel_area_cm2, 3)
    torso_cm2 = round(torso_pixels * pixel_area_cm2, 3)

    return {
        "status": "done",
        "slice_index": slice_index,
        "l3_slice_count": int(end - start + 1),
        "pixel_area_cm2": round(pixel_area_cm2, 6),
        "subcutaneous_fat_pixels": sat_pixels,
        "subcutaneous_fat_area_cm2": sat_cm2,
        "torso_fat_pixels": torso_pixels,
        "torso_fat_area_cm2": torso_cm2,
        "torso_to_subcutaneous_ratio": _ratio(torso_cm2, sat_cm2),
    }
