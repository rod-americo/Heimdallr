#!/usr/bin/env python3
"""Shared helpers for opportunistic bone-health metric jobs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from nibabel.orientations import aff2axcodes

from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_dir,
    study_id_json,
    study_metadata_json,
    study_nifti,
    study_results_json,
)


_OPPOSITE_DIRECTION = {
    "L": "R",
    "R": "L",
    "A": "P",
    "P": "A",
    "S": "I",
    "I": "S",
}

_DIRECTION_FAMILY = {
    "L": "lr",
    "R": "lr",
    "A": "ap",
    "P": "ap",
    "S": "si",
    "I": "si",
}


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--case-id", required=True, help="Study case identifier.")
    parser.add_argument(
        "--job-config-json",
        default="{}",
        help="JSON object with job-level configuration.",
    )
    return parser.parse_args()


def load_job_config(raw_json: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid --job-config-json payload: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Job configuration must be a JSON object")
    return parsed


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_case_json_bundle(case_id: str) -> dict[str, Any]:
    return {
        "id_json": read_json(study_id_json(case_id)),
        "metadata_json": read_json(study_metadata_json(case_id)),
        "results_json": read_json(study_results_json(case_id)),
    }


def resolve_canonical_nifti(case_id: str) -> Path | None:
    canonical = study_nifti(case_id)
    if canonical.exists():
        return canonical

    series_dir = study_dir(case_id) / "derived" / "series"
    if not series_dir.exists():
        return None
    candidates = sorted(series_dir.glob("*.nii.gz"))
    return candidates[0] if candidates else None


def load_nifti_mask(mask_path: Path) -> tuple[nib.Nifti1Image, np.ndarray]:
    image = nib.load(str(mask_path))
    return image, np.asarray(image.get_fdata(), dtype=np.float32) > 0


def load_ct_volume(ct_path: Path) -> tuple[nib.Nifti1Image, np.ndarray]:
    image = nib.load(str(ct_path))
    return image, np.asarray(image.get_fdata(), dtype=np.float32)


def affine_axis_codes(affine: np.ndarray) -> tuple[str, str, str]:
    codes = tuple(str(code) for code in aff2axcodes(affine))
    if len(codes) != 3 or any(code not in _DIRECTION_FAMILY for code in codes):
        raise RuntimeError(f"Unsupported affine axis codes: {codes}")
    return codes


def plane_source_axis_codes(affine: np.ndarray, plane_axis: str) -> tuple[str, str]:
    axis_codes = affine_axis_codes(affine)
    if plane_axis == "z":
        return axis_codes[0], axis_codes[1]
    if plane_axis == "x":
        return axis_codes[1], axis_codes[2]
    if plane_axis == "y":
        return axis_codes[0], axis_codes[2]
    raise ValueError(f"Unsupported plane axis: {plane_axis}")


def _display_axis_transform(
    source_axis_codes: tuple[str, str],
    *,
    desired_row_code: str,
    desired_col_code: str,
) -> tuple[bool, bool, bool, tuple[int, int]]:
    row_code, col_code = source_axis_codes
    desired_row_family = _DIRECTION_FAMILY[desired_row_code]
    desired_col_family = _DIRECTION_FAMILY[desired_col_code]

    if (
        _DIRECTION_FAMILY[row_code] == desired_row_family
        and _DIRECTION_FAMILY[col_code] == desired_col_family
    ):
        transpose = False
        display_codes = (row_code, col_code)
        spacing_order = (0, 1)
    elif (
        _DIRECTION_FAMILY[row_code] == desired_col_family
        and _DIRECTION_FAMILY[col_code] == desired_row_family
    ):
        transpose = True
        display_codes = (col_code, row_code)
        spacing_order = (1, 0)
    else:
        raise RuntimeError(
            f"Cannot orient plane with source axis codes {source_axis_codes} "
            f"to desired display directions {(desired_row_code, desired_col_code)}"
        )

    row_display_code, col_display_code = display_codes
    if row_display_code == desired_row_code:
        flip_rows = False
    elif row_display_code == _OPPOSITE_DIRECTION[desired_row_code]:
        flip_rows = True
    else:
        raise RuntimeError(
            f"Cannot orient plane row axis {row_display_code} to {desired_row_code}"
        )

    if col_display_code == desired_col_code:
        flip_cols = False
    elif col_display_code == _OPPOSITE_DIRECTION[desired_col_code]:
        flip_cols = True
    else:
        raise RuntimeError(
            f"Cannot orient plane col axis {col_display_code} to {desired_col_code}"
        )

    return transpose, flip_rows, flip_cols, spacing_order


def reorient_display_array(
    array: np.ndarray,
    *,
    source_axis_codes: tuple[str, str],
    desired_row_code: str,
    desired_col_code: str,
) -> np.ndarray:
    arr = np.asarray(array)
    if arr.ndim not in (2, 3):
        raise ValueError(f"Expected 2D or 3D display array, got shape {arr.shape}")

    transpose, flip_rows, flip_cols, _spacing_order = _display_axis_transform(
        source_axis_codes,
        desired_row_code=desired_row_code,
        desired_col_code=desired_col_code,
    )
    if transpose:
        arr = arr.T if arr.ndim == 2 else np.transpose(arr, (1, 0, 2))
    if flip_rows:
        arr = np.flip(arr, axis=0)
    if flip_cols:
        arr = np.flip(arr, axis=1)
    return np.ascontiguousarray(arr)


def reorient_display_spacing_mm(
    spacing_mm: tuple[float, float],
    *,
    source_axis_codes: tuple[str, str],
    desired_row_code: str,
    desired_col_code: str,
) -> tuple[float, float]:
    _transpose, _flip_rows, _flip_cols, spacing_order = _display_axis_transform(
        source_axis_codes,
        desired_row_code=desired_row_code,
        desired_col_code=desired_col_code,
    )
    return (
        float(spacing_mm[spacing_order[0]]),
        float(spacing_mm[spacing_order[1]]),
    )


def display_aspect_from_spacing_mm(spacing_mm: tuple[float, float]) -> float:
    row_spacing_mm = float(spacing_mm[0])
    col_spacing_mm = float(spacing_mm[1])
    if row_spacing_mm <= 0 or col_spacing_mm <= 0:
        return 1.0
    return row_spacing_mm / col_spacing_mm


def mask_complete_along_axis(mask: np.ndarray, axis: int) -> bool:
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


def center_slice_index(mask: np.ndarray) -> int | None:
    z_indices = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return None
    return int(z_indices[len(z_indices) // 2])


def sagittal_plane_from_mask(mask: np.ndarray) -> tuple[np.ndarray | None, int | None, str | None]:
    mask_bool = np.asarray(mask, dtype=bool)
    coords = np.argwhere(mask_bool)
    if coords.size == 0:
        return None, None, None

    x_min, y_min, _ = coords.min(axis=0)
    x_max, y_max, _ = coords.max(axis=0)
    x_span = int(x_max - x_min + 1)
    y_span = int(y_max - y_min + 1)

    if x_span <= y_span:
        plane_index = int(round((x_min + x_max) / 2.0))
        return np.asarray(mask_bool[plane_index, :, :], dtype=bool), plane_index, "x"

    plane_index = int(round((y_min + y_max) / 2.0))
    return np.asarray(mask_bool[:, plane_index, :], dtype=bool), plane_index, "y"


def extract_plane(data: np.ndarray, plane_axis: str, plane_index: int) -> np.ndarray:
    array = np.asarray(data)
    if plane_axis == "x":
        return np.asarray(array[plane_index, :, :])
    if plane_axis == "y":
        return np.asarray(array[:, plane_index, :])
    raise ValueError(f"Unsupported plane axis: {plane_axis}")


def sagittal_plane_spacing_mm(
    spacing_mm: tuple[float, float, float],
    plane_axis: str,
) -> tuple[float, float]:
    if plane_axis == "x":
        return float(spacing_mm[1]), float(spacing_mm[2])
    if plane_axis == "y":
        return float(spacing_mm[0]), float(spacing_mm[2])
    raise ValueError(f"Unsupported plane axis: {plane_axis}")


def build_l1_axial_roi(mask_l1: np.ndarray, spacing_mm: tuple[float, float, float]) -> tuple[np.ndarray | None, dict[str, Any]]:
    from scipy.ndimage import binary_erosion, center_of_mass, label as ndlabel

    center_z = center_slice_index(mask_l1)
    if center_z is None:
        return None, {"status": "empty_mask"}

    mask_2d = np.asarray(mask_l1[:, :, center_z], dtype=bool)
    in_plane_spacing = min(float(spacing_mm[0]), float(spacing_mm[1]))
    erosion_iters = max(1, int(5.0 / max(in_plane_spacing, 1e-6)))
    eroded_2d = binary_erosion(mask_2d, iterations=erosion_iters)

    labeled, num_features = ndlabel(eroded_2d)
    if num_features > 1:
        component_sizes = [np.sum(labeled == i) for i in range(1, num_features + 1)]
        largest = int(np.argmax(component_sizes)) + 1
        eroded_2d = labeled == largest

    if not np.any(eroded_2d):
        return None, {"status": "empty_eroded_mask", "slice_index": center_z}

    full_com_x, full_com_y = center_of_mass(mask_2d)
    body_com_x, body_com_y = center_of_mass(eroded_2d)

    x_indices, y_indices = np.where(eroded_2d)
    x_min, x_max = int(x_indices.min()), int(x_indices.max())
    y_min, y_max = int(y_indices.min()), int(y_indices.max())

    diff_x = abs(float(full_com_x) - float(body_com_x))
    diff_y = abs(float(full_com_y) - float(body_com_y))

    rx = max((x_max - x_min) * 0.70 / 2.0, 1.0)
    ry = max((y_max - y_min) * 0.40 / 2.0, 1.0)

    if diff_y > diff_x:
        anterior_is_larger_y = body_com_y > full_com_y
        center_x = (x_min + x_max) / 2.0
        center_y = y_max - (y_max - y_min) * 0.25 if anterior_is_larger_y else y_min + (y_max - y_min) * 0.25
    else:
        anterior_is_larger_x = body_com_x > full_com_x
        center_y = (y_min + y_max) / 2.0
        center_x = x_max - (x_max - x_min) * 0.25 if anterior_is_larger_x else x_min + (x_max - x_min) * 0.25

    x_grid, y_grid = np.ogrid[:mask_2d.shape[0], :mask_2d.shape[1]]
    ellipse = ((x_grid - center_x) ** 2 / rx**2) + ((y_grid - center_y) ** 2 / ry**2) <= 1
    roi_mask = np.asarray(ellipse & eroded_2d, dtype=bool)
    if not np.any(roi_mask):
        return None, {"status": "empty_roi_mask", "slice_index": center_z}

    return roi_mask, {
        "status": "ok",
        "slice_index": center_z,
        "roi_center_xy": {"x": round(float(center_x), 2), "y": round(float(center_y), 2)},
        "roi_radius_xy": {"x": round(float(rx), 2), "y": round(float(ry), 2)},
    }


def build_l1_sagittal_roi(
    mask_l1: np.ndarray,
    spacing_mm: tuple[float, float, float],
    erosion_mm: float = 5.0,
    roi_radius_mm: float = 6.0,
) -> tuple[np.ndarray | None, dict[str, Any]]:
    from scipy.ndimage import binary_erosion, distance_transform_edt, label as ndlabel

    plane_mask, plane_index, plane_axis = sagittal_plane_from_mask(mask_l1)
    if plane_mask is None or plane_index is None or plane_axis is None:
        return None, {"status": "empty_mask"}

    plane_spacing = sagittal_plane_spacing_mm(spacing_mm, plane_axis)
    min_spacing = max(min(plane_spacing), 1e-6)
    erosion_iters = max(1, int(round(float(erosion_mm) / min_spacing)))
    ap_profile = plane_mask.sum(axis=1).astype(np.float64)
    edge_width = max(1, int(round(ap_profile.size * 0.18)))
    low_edge = float(np.median(ap_profile[:edge_width]))
    high_edge = float(np.median(ap_profile[-edge_width:]))
    anterior_is_low_index = high_edge > low_edge

    eroded_2d = binary_erosion(plane_mask, iterations=erosion_iters)

    labeled, num_features = ndlabel(eroded_2d)
    if num_features > 1:
        selected_component = None
        selected_score = None
        for component_idx in range(1, num_features + 1):
            component_mask = labeled == component_idx
            coords = np.argwhere(component_mask)
            if coords.size == 0:
                continue
            mean_row = float(np.mean(coords[:, 0]))
            score = mean_row if anterior_is_low_index else -mean_row
            if selected_score is None or score < selected_score:
                selected_score = score
                selected_component = component_idx
        if selected_component is not None:
            eroded_2d = labeled == selected_component

    if not np.any(eroded_2d):
        return None, {
            "status": "empty_eroded_mask",
            "plane": "sagittal",
            "plane_axis": plane_axis,
            "plane_index": plane_index,
        }

    row_indices, col_indices = np.where(eroded_2d)
    row_min, row_max = int(row_indices.min()), int(row_indices.max())
    col_min, col_max = int(col_indices.min()), int(col_indices.max())
    ap_span = max(1, row_max - row_min + 1)
    si_span = max(1, col_max - col_min + 1)

    # Place the ROI slightly anterior, centered around segments 2-3 out of 7.
    ap_center_fraction = 2.5 / 7.0
    if anterior_is_low_index:
        center_row = row_min + (ap_span - 1) * ap_center_fraction
    else:
        center_row = row_max - (ap_span - 1) * ap_center_fraction
    center_col = (col_min + col_max) / 2.0

    distance_mm = distance_transform_edt(eroded_2d, sampling=plane_spacing)
    max_inscribed_radius_mm = float(np.max(distance_mm))
    if max_inscribed_radius_mm <= 0.0:
        return None, {
            "status": "empty_distance_core",
            "plane": "sagittal",
            "plane_axis": plane_axis,
            "plane_index": plane_index,
        }

    row_grid, col_grid = np.ogrid[:eroded_2d.shape[0], :eroded_2d.shape[1]]
    target_distance_mm = np.sqrt(
        ((row_grid - center_row) * plane_spacing[0]) ** 2
        + ((col_grid - center_col) * plane_spacing[1]) ** 2
    )
    center_score = np.where(eroded_2d, distance_mm - (0.12 * target_distance_mm), -np.inf)
    center_row, center_col = np.unravel_index(int(np.argmax(center_score)), center_score.shape)

    requested_ap_radius_mm = max(float(roi_radius_mm) * 0.85, min_spacing)
    requested_si_radius_mm = max(float(roi_radius_mm) * 1.15, min_spacing)
    effective_ap_radius_mm = min(requested_ap_radius_mm, max_inscribed_radius_mm * 0.95)
    effective_si_radius_mm = min(requested_si_radius_mm, max_inscribed_radius_mm * 1.20)
    effective_ap_radius_mm = max(effective_ap_radius_mm, min_spacing * 0.5)
    effective_si_radius_mm = max(effective_si_radius_mm, min_spacing * 0.5)

    ellipse = (
        ((row_grid - center_row) * plane_spacing[0]) ** 2 / max(effective_ap_radius_mm**2, 1e-6)
        + ((col_grid - center_col) * plane_spacing[1]) ** 2 / max(effective_si_radius_mm**2, 1e-6)
    ) <= 1.0
    roi_mask = np.asarray(ellipse & eroded_2d, dtype=bool)
    if not np.any(roi_mask):
        return None, {
            "status": "empty_roi_mask",
            "plane": "sagittal",
            "plane_axis": plane_axis,
            "plane_index": plane_index,
        }

    return roi_mask, {
        "status": "ok",
        "plane": "sagittal",
        "plane_axis": plane_axis,
        "plane_index": plane_index,
        "roi_center_2d": {"row": round(float(center_row), 2), "col": round(float(center_col), 2)},
        "roi_radius_mm": {
            "ap": round(float(effective_ap_radius_mm), 2),
            "si": round(float(effective_si_radius_mm), 2),
        },
        "max_inscribed_radius_mm": round(float(max_inscribed_radius_mm), 2),
        "roi_ap_center_fraction": round(float(ap_center_fraction), 4),
        "anterior_is_low_index": bool(anterior_is_low_index),
        "plane_spacing_mm": {
            "row": round(float(plane_spacing[0]), 4),
            "col": round(float(plane_spacing[1]), 4),
        },
    }


def save_png_overlay(
    ct_slice: np.ndarray,
    overlay_mask: np.ndarray,
    output_path: Path,
    title: str,
    summary_lines: list[str],
    mask_outline: np.ndarray | None = None,
    overlay_cmap: str = "cool",
    vmin: float = -250.0,
    vmax: float = 1250.0,
) -> None:
    rotated_ct = np.rot90(np.asarray(ct_slice, dtype=np.float32))
    rotated_overlay = np.rot90(np.asarray(overlay_mask, dtype=bool))
    rotated_outline = np.rot90(np.asarray(mask_outline, dtype=bool)) if mask_outline is not None else None

    fig, ax = plt.subplots(figsize=(7, 7))
    ax.imshow(rotated_ct, cmap="gray", vmin=vmin, vmax=vmax, interpolation="nearest")

    if rotated_overlay.any():
        masked = np.ma.masked_where(~rotated_overlay, rotated_overlay.astype(np.uint8))
        ax.imshow(masked, cmap=overlay_cmap, alpha=0.55, interpolation="nearest")
        ax.contour(rotated_overlay, levels=[0.5], colors=["#66e0ff"], linewidths=1.1)

    if rotated_outline is not None and rotated_outline.any():
        ax.contour(rotated_outline, levels=[0.5], colors=["#ffd166"], linewidths=0.9)

    ax.set_title(title, fontsize=13)
    ax.text(
        0.03,
        0.97,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="white",
        bbox={
            "boxstyle": "round,pad=0.4",
            "facecolor": "black",
            "alpha": 0.55,
            "edgecolor": "none",
        },
    )
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def metric_output_dir(case_id: str, metric_key: str) -> tuple[Path, Path, Path]:
    case_dir = study_dir(case_id)
    metric_dir = study_artifacts_dir(case_id) / "metrics" / metric_key
    metric_dir.mkdir(parents=True, exist_ok=True)
    return case_dir, metric_dir, metric_dir / "result.json"


def write_payload(result_path: Path, payload: dict[str, Any]) -> None:
    result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
