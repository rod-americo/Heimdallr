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

"""Bone health helpers for opportunistic L1 screening.

The helpers in this module are intentionally pure and operate on numpy arrays
and plain mappings only. They are designed to be integrated into the existing
metrics pipeline without forcing changes in the rest of the processing stack.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
from scipy.ndimage import binary_erosion, center_of_mass, label as ndlabel

__all__ = [
    "extract_study_technique_context",
    "calculate_mask_hu_statistics",
    "build_l1_trabecular_roi_mask",
    "compute_l1_volumetric_metrics",
    "compute_l1_fracture_screen",
    "build_bone_health_qc_flags",
    "classify_l1_hu",
    "build_opportunistic_osteoporosis_composite",
]

DEFAULT_NORMAL_HU_CUTOFF = 160.0
DEFAULT_OSTEOPENIA_HU_CUTOFF = 100.0
DEFAULT_MIN_TRABECULAR_VOXELS = 25
DEFAULT_MAX_SLICE_THICKNESS_MM = 3.0
DEFAULT_ALLOWED_KVP_RANGE = (80.0, 140.0)


def _as_mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            value = mapping[key]
            if value not in (None, "", "Unknown"):
                return value
    return None


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _normalize_spacing(spacing_mm: Sequence[float] | None, ndim: int = 3) -> tuple[float, ...]:
    if spacing_mm is None:
        return tuple(1.0 for _ in range(ndim))

    values = []
    for item in spacing_mm:
        parsed = _parse_float(item)
        if parsed is not None:
            values.append(parsed)

    if not values:
        return tuple(1.0 for _ in range(ndim))

    if len(values) >= ndim:
        return tuple(values[:ndim])

    if len(values) == 1:
        return tuple(values[0] for _ in range(ndim))

    # Keep the last available spacing for the remaining axes.
    while len(values) < ndim:
        values.append(values[-1])
    return tuple(values)


def _parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, np.integer)):
        return bool(int(value))

    text = str(value).strip().lower()
    if not text:
        return None

    truthy = {
        "1",
        "true",
        "t",
        "yes",
        "y",
        "with contrast",
        "contrast",
        "enhanced",
        "postcontrast",
        "arterial",
        "venous",
        "delayed",
    }
    falsy = {
        "0",
        "false",
        "f",
        "no",
        "n",
        "native",
        "noncontrast",
        "non-contrast",
        "unenhanced",
        "without contrast",
        "w/o contrast",
    }

    if text in truthy or any(token in text for token in truthy if " " in token):
        return True
    if text in falsy or any(token in text for token in falsy if " " in token):
        return False
    return None


def _normalize_modality(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text.upper()


def _phase_implies_contrast(phase: Any) -> bool | None:
    if phase is None:
        return None
    text = str(phase).strip().lower()
    if not text:
        return None
    if any(token in text for token in ("native", "noncontrast", "unenhanced", "without contrast")):
        return False
    if any(token in text for token in ("arterial", "venous", "delayed", "portal", "contrast", "enhanced", "post")):
        return True
    return None


def extract_study_technique_context(
    id_data: Mapping[str, Any] | None = None,
    results: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Extract normalized technical context from study metadata and results.

    The function prefers `results` over `id_data` when both provide the same
    field, because `results` usually reflects the selected series after
    pipeline-specific normalization.
    """

    id_data = _as_mapping(id_data)
    results = _as_mapping(results)

    modality = _normalize_modality(
        _first_present(results, "modality", "Modality") or _first_present(id_data, "Modality", "modality")
    )

    kvp_raw = _first_present(
        results,
        "kvp",
        "KVP",
        "tube_voltage",
        "TubeVoltage",
    ) or _first_present(
        id_data,
        "KVP",
        "kvp",
        "tube_voltage",
        "TubeVoltage",
    )
    kvp = _parse_float(kvp_raw)

    slice_thickness_raw = _first_present(
        results,
        "slice_thickness_mm",
        "SliceThickness",
        "slice_thickness",
        "SpacingBetweenSlices",
    ) or _first_present(
        id_data,
        "SliceThickness",
        "slice_thickness_mm",
        "slice_thickness",
        "SpacingBetweenSlices",
    )
    slice_thickness_mm = _parse_float(slice_thickness_raw)

    contrast_raw = _first_present(
        results,
        "contrast",
        "has_contrast",
        "Contrast",
        "ContrastPhase",
        "contrast_phase",
        "phase",
        "SelectedPhase",
    ) or _first_present(
        id_data,
        "contrast",
        "has_contrast",
        "Contrast",
        "ContrastPhase",
        "contrast_phase",
        "phase",
        "SelectedPhase",
    )

    contrast = _parse_bool(contrast_raw)
    if contrast is None:
        contrast = _phase_implies_contrast(contrast_raw)

    if contrast is None:
        phase = _first_present(results, "SelectedPhase", "phase", "ContrastPhase") or _first_present(
            id_data, "SelectedPhase", "phase", "ContrastPhase"
        )
        contrast = _phase_implies_contrast(phase)

    manufacturer = _first_present(results, "manufacturer", "Manufacturer") or _first_present(
        id_data, "Manufacturer", "manufacturer"
    )
    model = _first_present(results, "manufacturer_model", "ManufacturerModelName", "model") or _first_present(
        id_data, "ManufacturerModelName", "manufacturer_model", "model"
    )
    body_part = _first_present(results, "body_part_examined", "BodyPartExamined") or _first_present(
        id_data, "BodyPartExamined", "body_part_examined"
    )

    spacing = _first_present(results, "spacing_mm", "Spacing") or _first_present(
        id_data, "spacing_mm", "Spacing"
    )
    if isinstance(spacing, Sequence) and not isinstance(spacing, (str, bytes)):
        spacing_mm = tuple(_parse_float(value) or 1.0 for value in spacing)
    else:
        spacing_mm = None

    return {
        "modality": modality,
        "kvp_raw": kvp_raw,
        "kvp": kvp,
        "contrast_raw": contrast_raw,
        "contrast": contrast,
        "slice_thickness_raw": slice_thickness_raw,
        "slice_thickness_mm": slice_thickness_mm,
        "manufacturer": manufacturer,
        "manufacturer_model": model,
        "body_part_examined": body_part,
        "spacing_mm": spacing_mm,
    }


def calculate_mask_hu_statistics(ct: np.ndarray, mask: np.ndarray) -> dict[str, Any]:
    """Return HU mean/std and voxel count for a mask over a CT volume."""

    ct_arr = np.asarray(ct, dtype=np.float32)
    mask_bool = np.asarray(mask, dtype=bool)

    if ct_arr.shape != mask_bool.shape:
        raise ValueError(f"Shape mismatch: CT {ct_arr.shape} vs mask {mask_bool.shape}")

    voxels = ct_arr[mask_bool]
    voxel_count = int(voxels.size)
    if voxel_count == 0:
        return {"voxel_count": 0, "mean_hu": None, "std_hu": None}

    return {
        "voxel_count": voxel_count,
        "mean_hu": round(float(np.mean(voxels)), 2),
        "std_hu": round(float(np.std(voxels)), 2),
    }


def _keep_largest_component_2d(mask_2d: np.ndarray) -> np.ndarray:
    mask_bool = np.asarray(mask_2d, dtype=bool)
    if not np.any(mask_bool):
        return mask_bool

    labeled, num_features = ndlabel(mask_bool, structure=np.ones((3, 3), dtype=np.uint8))
    if num_features <= 1:
        return mask_bool

    full_com = center_of_mass(mask_bool)
    best_label = 1
    best_key = (-1, float("inf"))

    for component_id in range(1, num_features + 1):
        component = labeled == component_id
        area = int(component.sum())
        if area == 0:
            continue
        component_com = center_of_mass(component)
        distance = float(np.linalg.norm(np.asarray(component_com) - np.asarray(full_com)))
        key = (area, -distance)
        if key > best_key:
            best_key = key
            best_label = component_id

    return labeled == best_label


def _keep_largest_component_3d(mask_3d: np.ndarray) -> np.ndarray:
    mask_bool = np.asarray(mask_3d, dtype=bool)
    if not np.any(mask_bool):
        return mask_bool

    labeled, num_features = ndlabel(mask_bool, structure=np.ones((3, 3, 3), dtype=np.uint8))
    if num_features <= 1:
        return mask_bool

    component_sizes = [int(np.sum(labeled == component_id)) for component_id in range(1, num_features + 1)]
    best_label = int(np.argmax(component_sizes)) + 1
    return labeled == best_label


def build_l1_trabecular_roi_mask(
    mask: np.ndarray,
    spacing_mm: Sequence[float] | None = None,
    erosion_mm: float = 5.0,
    slice_axis: int = -1,
) -> np.ndarray:
    """Build a trabecular ROI mask from the segmented L1 vertebra.

    The heuristic combines 3D erosion with a per-slice largest-component
    selection to suppress posterior elements and other small attachments.
    """

    mask_bool = np.asarray(mask, dtype=bool)
    if mask_bool.ndim != 3:
        raise ValueError(f"Expected a 3D mask, got {mask_bool.ndim}D")
    if not np.any(mask_bool):
        return mask_bool.copy()

    spacing = _normalize_spacing(spacing_mm, 3)
    min_spacing = max(min(spacing), 1e-6)
    erosion_iterations = max(1, int(round(float(erosion_mm) / min_spacing)))

    eroded = binary_erosion(mask_bool, iterations=erosion_iterations)
    if not np.any(eroded):
        eroded = mask_bool.copy()

    moved = np.moveaxis(eroded, slice_axis, -1)
    selected = np.zeros_like(moved, dtype=bool)

    for index in range(moved.shape[-1]):
        selected[..., index] = _keep_largest_component_2d(moved[..., index])

    selected = np.moveaxis(selected, -1, slice_axis)
    selected = _keep_largest_component_3d(selected)

    if np.count_nonzero(selected) < DEFAULT_MIN_TRABECULAR_VOXELS and np.count_nonzero(eroded) > 0:
        return np.asarray(eroded, dtype=bool)

    return selected


def compute_l1_volumetric_metrics(
    ct: np.ndarray,
    mask: np.ndarray,
    spacing_mm: Sequence[float] | None = None,
    erosion_mm: float = 5.0,
    slice_axis: int = -1,
) -> dict[str, Any]:
    """Compute full-mask and trabecular-volume HU metrics for L1."""

    full_stats = calculate_mask_hu_statistics(ct, mask)
    trabecular_mask = build_l1_trabecular_roi_mask(
        mask,
        spacing_mm=spacing_mm,
        erosion_mm=erosion_mm,
        slice_axis=slice_axis,
    )
    trabecular_stats = calculate_mask_hu_statistics(ct, trabecular_mask)

    full_voxel_count = int(full_stats["voxel_count"])
    trabecular_voxel_count = int(trabecular_stats["voxel_count"])
    trabecular_fraction = (
        round(trabecular_voxel_count / full_voxel_count, 4) if full_voxel_count > 0 else None
    )

    return {
        "bone_health_l1_volumetric_full_hu_mean": full_stats["mean_hu"],
        "bone_health_l1_volumetric_full_hu_std": full_stats["std_hu"],
        "bone_health_l1_volumetric_full_voxel_count": full_voxel_count,
        "bone_health_l1_volumetric_trabecular_hu_mean": trabecular_stats["mean_hu"],
        "bone_health_l1_volumetric_trabecular_hu_std": trabecular_stats["std_hu"],
        "bone_health_l1_volumetric_trabecular_voxel_count": trabecular_voxel_count,
        "bone_health_l1_volumetric_trabecular_fraction": trabecular_fraction,
        "bone_health_l1_volumetric_roi_method": "eroded_largest_component_slice_filter",
    }


def compute_l1_fracture_screen(
    mask: np.ndarray,
    spacing_mm: Sequence[float] | None = None,
    min_voxel_count: int = 40,
    asymmetry_threshold: float = 0.80,
) -> dict[str, Any]:
    """Compute a lightweight vertebral compression screen from the L1 mask.

    The heuristic uses PCA-like principal axes over the mask coordinates and
    compares superior-inferior extents across three AP bins. This does not try
    to diagnose fracture; it only yields a structured suspicion flag.
    """

    mask_bool = np.asarray(mask, dtype=bool)
    if mask_bool.ndim != 3:
        raise ValueError(f"Expected a 3D mask, got {mask_bool.ndim}D")

    voxel_count = int(np.count_nonzero(mask_bool))
    if voxel_count < min_voxel_count:
        return {
            "bone_health_l1_fracture_screen_status": "indeterminate",
            "bone_health_l1_fracture_screen_voxel_count": voxel_count,
            "bone_health_l1_fracture_screen_suspicion": None,
            "bone_health_l1_fracture_screen_classification": "indeterminate",
            "bone_health_l1_fracture_screen_min_height_ratio": None,
            "bone_health_l1_fracture_screen_region_heights_mm": None,
        }

    spacing = np.asarray(_normalize_spacing(spacing_mm, 3), dtype=np.float64)
    coords = np.argwhere(mask_bool).astype(np.float64)
    coords_mm = coords * spacing
    centered = coords_mm - coords_mm.mean(axis=0, keepdims=True)

    try:
        _, _, vh = np.linalg.svd(centered, full_matrices=False)
    except np.linalg.LinAlgError:
        return {
            "bone_health_l1_fracture_screen_status": "indeterminate",
            "bone_health_l1_fracture_screen_voxel_count": voxel_count,
            "bone_health_l1_fracture_screen_suspicion": None,
            "bone_health_l1_fracture_screen_classification": "indeterminate",
            "bone_health_l1_fracture_screen_min_height_ratio": None,
            "bone_health_l1_fracture_screen_region_heights_mm": None,
        }

    si_axis = vh[0]
    ap_axis = vh[1] if vh.shape[0] > 1 else vh[0]
    proj_si = centered @ si_axis
    proj_ap = centered @ ap_axis

    q1, q2 = np.quantile(proj_ap, [1.0 / 3.0, 2.0 / 3.0])
    region_masks = [
        proj_ap <= q1,
        (proj_ap > q1) & (proj_ap <= q2),
        proj_ap > q2,
    ]

    region_heights = []
    for region in region_masks:
        if not np.any(region):
            region_heights.append(0.0)
            continue
        region_proj = proj_si[region]
        region_heights.append(round(float(region_proj.max() - region_proj.min()), 2))

    max_height = max(region_heights) if region_heights else 0.0
    min_height = min(region_heights) if region_heights else 0.0
    min_height_ratio = round(min_height / max_height, 4) if max_height > 0 else None
    middle_ratio = round(region_heights[1] / max_height, 4) if max_height > 0 else None
    end_ratio = round(min(region_heights[0], region_heights[2]) / max(region_heights[0], region_heights[2]), 4) if max(
        region_heights[0], region_heights[2]
    ) > 0 else None

    suspicion = False
    reasons: list[str] = []
    if min_height_ratio is not None and min_height_ratio < asymmetry_threshold:
        suspicion = True
        reasons.append("height_asymmetry")
    if middle_ratio is not None and middle_ratio < asymmetry_threshold:
        suspicion = True
        reasons.append("middle_height_loss")
    if end_ratio is not None and end_ratio < asymmetry_threshold:
        suspicion = True
        reasons.append("end_height_asymmetry")

    if suspicion:
        classification = "suspected_fracture"
    else:
        classification = "no_suspicion"

    return {
        "bone_health_l1_fracture_screen_status": "complete",
        "bone_health_l1_fracture_screen_voxel_count": voxel_count,
        "bone_health_l1_fracture_screen_suspicion": suspicion,
        "bone_health_l1_fracture_screen_classification": classification,
        "bone_health_l1_fracture_screen_min_height_ratio": min_height_ratio,
        "bone_health_l1_fracture_screen_middle_height_ratio": middle_ratio,
        "bone_health_l1_fracture_screen_end_height_ratio": end_ratio,
        "bone_health_l1_fracture_screen_region_heights_mm": region_heights,
        "bone_health_l1_fracture_screen_reasons": reasons,
    }


def build_bone_health_qc_flags(
    context: Mapping[str, Any] | None,
    full_mask_voxel_count: int,
    trabecular_voxel_count: int | None,
    mask_complete: bool,
    strict: bool = False,
    min_voxel_count: int = DEFAULT_MIN_TRABECULAR_VOXELS,
    allowed_kvp_range: tuple[float, float] = DEFAULT_ALLOWED_KVP_RANGE,
    max_slice_thickness_mm: float = DEFAULT_MAX_SLICE_THICKNESS_MM,
) -> dict[str, Any]:
    """Build QC flags for opportunistic bone health jobs."""

    context = _as_mapping(context)
    modality = _normalize_modality(context.get("modality"))
    kvp = _parse_float(context.get("kvp"))
    contrast = _parse_bool(context.get("contrast"))
    slice_thickness_mm = _parse_float(context.get("slice_thickness_mm"))

    reference_voxel_count = trabecular_voxel_count if trabecular_voxel_count is not None else full_mask_voxel_count
    voxel_count_ok = int(reference_voxel_count or 0) >= int(min_voxel_count)

    kvp_in_range: bool | None
    if kvp is None:
        kvp_in_range = None
    else:
        kvp_in_range = allowed_kvp_range[0] <= kvp <= allowed_kvp_range[1]

    contrast_present = contrast
    if contrast_present is None:
        contrast_present = None

    slice_thickness_ok: bool | None
    if slice_thickness_mm is None:
        slice_thickness_ok = None
    else:
        slice_thickness_ok = slice_thickness_mm <= max_slice_thickness_mm

    qc_reasons: list[str] = []
    if modality is not None and modality != "CT":
        qc_reasons.append("non_ct_modality")
    if not mask_complete:
        qc_reasons.append("mask_incomplete")
    if not voxel_count_ok:
        qc_reasons.append("low_voxel_count")
    if kvp_in_range is False:
        qc_reasons.append("kvp_out_of_range")
    if contrast_present is True:
        qc_reasons.append("contrast_present")
    if slice_thickness_ok is False:
        qc_reasons.append("slice_thickness_high")

    core_pass = (modality in (None, "CT")) and mask_complete and voxel_count_ok
    advisory_pass = True
    if strict:
        advisory_pass = (kvp_in_range is not False) and (contrast_present is not True) and (slice_thickness_ok is not False)
    qc_pass = bool(core_pass and advisory_pass)

    return {
        "bone_health_qc_pass": qc_pass,
        "bone_health_qc_modality_is_ct": modality in (None, "CT"),
        "bone_health_qc_mask_complete": bool(mask_complete),
        "bone_health_qc_voxel_count_ok": bool(voxel_count_ok),
        "bone_health_qc_reference_voxel_count": int(reference_voxel_count or 0),
        "bone_health_qc_kvp_in_range": kvp_in_range,
        "bone_health_qc_contrast_present": contrast_present,
        "bone_health_qc_slice_thickness_ok": slice_thickness_ok,
        "bone_health_qc_reasons": qc_reasons,
    }


def classify_l1_hu(
    hu_mean: float | None,
    normal_cutoff: float = DEFAULT_NORMAL_HU_CUTOFF,
    osteopenia_cutoff: float = DEFAULT_OSTEOPENIA_HU_CUTOFF,
) -> str:
    """Classify L1 HU into a simple opportunistic screening label."""

    if hu_mean is None:
        return "indeterminate"
    value = float(hu_mean)
    if value > normal_cutoff:
        return "normal"
    if value >= osteopenia_cutoff:
        return "osteopenia"
    return "osteoporosis"


def build_opportunistic_osteoporosis_composite(
    l1_trabecular_hu_mean: float | None = None,
    l1_full_hu_mean: float | None = None,
    fracture_suspicion: bool | None = None,
    qc_pass: bool | None = None,
    density_label: str | None = None,
) -> dict[str, Any]:
    """Build a simple composite opportunistic osteoporosis signal.

    The composite is intentionally conservative: it is a screening flag, not a
    diagnosis, and it prefers the trabecular HU estimate when available.
    """

    source_hu = l1_trabecular_hu_mean if l1_trabecular_hu_mean is not None else l1_full_hu_mean
    density_label = density_label or classify_l1_hu(source_hu)

    score = 0
    reasons: list[str] = []

    if density_label == "osteoporosis":
        score += 60
        reasons.append("low_hu")
    elif density_label == "osteopenia":
        score += 35
        reasons.append("borderline_hu")
    elif density_label == "normal":
        score += 5
        reasons.append("preserved_hu")

    if fracture_suspicion is True:
        score += 35
        reasons.append("fracture_suspicion")
    elif fracture_suspicion is None:
        reasons.append("fracture_unknown")

    if qc_pass is False:
        score = max(0, score - 20)
        reasons.append("qc_failed")
    elif qc_pass is None:
        reasons.append("qc_unknown")

    if source_hu is None and fracture_suspicion is not True:
        composite = "indeterminate"
    elif score >= 70:
        composite = "high"
    elif score >= 35:
        composite = "moderate"
    else:
        composite = "low"

    return {
        "opportunistic_osteoporosis_composite": composite,
        "opportunistic_osteoporosis_composite_score": int(min(score, 100)),
        "opportunistic_osteoporosis_composite_density_label": density_label,
        "opportunistic_osteoporosis_composite_reasons": reasons,
    }

