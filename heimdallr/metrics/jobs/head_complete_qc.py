#!/usr/bin/env python3
"""Validate complete head segmentation and normalize head CT geometry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from pydicom.uid import generate_uid
from nibabel.processing import resample_from_to
from scipy.ndimage import binary_dilation, binary_erosion

from heimdallr.metrics.head import (
    BRAIN_STRUCTURE_MASKS,
    HEAD_COMPONENT_MASKS,
    collect_mask_statuses,
    compute_mask_status,
    normalize_nifti_to_axial,
    normalize_nifti_to_brain_mask_geometry_isotropic,
    normalize_nifti_to_ras_isotropic,
    parse_normalization_spec,
)
from heimdallr.metrics.jobs._bone_job_common import (
    load_ct_volume,
    load_job_config,
    read_json,
    reorient_display_array,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import (
    create_secondary_capture_from_rgb,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._dicom_ct_series import create_derived_ct_series_from_nifti
from heimdallr.shared import settings
from heimdallr.shared.i18n import format_decimal, normalize_locale, translate
from heimdallr.shared.paths import study_metadata_json


BLEED_MASK_NAME = "intracerebral_hemorrhage"
STRUCTURE_SERIES_NUMBER = 9120
BLEED_SERIES_NUMBER = 9121
VOLUME_TABLE_SERIES_NUMBER = 9122
GEOMETRY_CT_SERIES_NUMBER = 9123
WINDOW_MIN = 0.0
WINDOW_MAX = 80.0
OVERLAY_SLICE_THICKNESS_MM = 3.0
BLEED_OVERLAY_SLICE_THICKNESS_MM = 5.0
STRUCTURE_COLORS = (
    (255, 99, 132),
    (54, 162, 235),
    (255, 206, 86),
    (75, 192, 192),
    (153, 102, 255),
    (255, 159, 64),
    (46, 204, 113),
    (231, 76, 60),
    (52, 152, 219),
    (241, 196, 15),
    (155, 89, 182),
    (26, 188, 156),
    (230, 126, 34),
    (149, 165, 166),
    (127, 140, 141),
    (192, 57, 43),
)
BLEED_COLOR = (255, 32, 32)


def _relpath(case_dir: Path, path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(case_dir))
    except ValueError:
        return str(path)


def _load_mask(mask_path: Path) -> np.ndarray:
    image = nib.load(str(mask_path))
    return np.asarray(image.get_fdata(), dtype=np.float32) > 0


def _load_case_metadata(case_id: str, case_dir: Path) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    merged.update(read_json(case_dir / "metadata" / "id.json"))
    merged.update(read_json(study_metadata_json(case_id)))
    return merged


def _artifact_locale(job_config: dict[str, Any]) -> str:
    return normalize_locale(job_config.get("locale") or settings.ARTIFACTS_LOCALE)


def _font(size: int) -> ImageFont.ImageFont:
    for candidate in (
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ):
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            return ImageFont.truetype(str(path), size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _ct_to_rgb(ct_slice: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(ct_slice, dtype=np.float32), WINDOW_MIN, WINDOW_MAX)
    scaled = ((clipped - WINDOW_MIN) / (WINDOW_MAX - WINDOW_MIN) * 255.0).astype(np.uint8)
    return np.repeat(scaled[..., None], 3, axis=2)


def _outline(mask_2d: np.ndarray) -> np.ndarray:
    mask = np.asarray(mask_2d, dtype=bool)
    if not mask.any():
        return mask
    return mask & ~binary_erosion(mask, iterations=1)


def _blend(rgb: np.ndarray, mask_2d: np.ndarray, color: tuple[int, int, int], *, alpha: float) -> np.ndarray:
    out = np.asarray(rgb, dtype=np.float32).copy()
    mask = np.asarray(mask_2d, dtype=bool)
    if not mask.any():
        return out.astype(np.uint8)
    color_arr = np.asarray(color, dtype=np.float32)
    out[mask] = ((1.0 - alpha) * out[mask]) + (alpha * color_arr)
    out[_outline(mask)] = color_arr
    return np.clip(out, 0, 255).astype(np.uint8)


def _blend_contour(
    rgb: np.ndarray,
    mask_2d: np.ndarray,
    color: tuple[int, int, int],
    *,
    alpha: float,
    outline_iterations: int = 2,
) -> np.ndarray:
    out = np.asarray(rgb, dtype=np.float32).copy()
    mask = np.asarray(mask_2d, dtype=bool)
    if not mask.any():
        return out.astype(np.uint8)
    color_arr = np.asarray(color, dtype=np.float32)
    out[mask] = ((1.0 - alpha) * out[mask]) + (alpha * color_arr)
    dilated = binary_dilation(mask, iterations=max(1, int(outline_iterations)))
    eroded = binary_erosion(mask, iterations=1)
    contour = dilated & ~eroded
    out[contour] = color_arr
    return np.clip(out, 0, 255).astype(np.uint8)


def _source_positions_mm(z_size: int, spacing_z: float) -> np.ndarray:
    return np.arange(int(z_size), dtype=np.float32) * float(spacing_z)


def _indices_for_slab(source_positions_mm: np.ndarray, center_mm: float, slab_mm: float) -> list[int]:
    half = float(slab_mm) / 2.0
    selected = np.where(
        (source_positions_mm >= center_mm - half)
        & (source_positions_mm <= center_mm + half)
    )[0]
    if selected.size == 0:
        selected = np.asarray([int(np.argmin(np.abs(source_positions_mm - center_mm)))])
    return [int(value) for value in selected.tolist()]


def _build_slabs(mask: np.ndarray, *, spacing_z: float, slab_mm: float) -> list[dict[str, Any]]:
    occupied = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if occupied.size == 0:
        return []
    positions = _source_positions_mm(mask.shape[2], spacing_z)
    first_center = max(0, int(np.floor(float(positions[int(occupied[0])]) / slab_mm)) - 1)
    last_center = int(np.ceil(float(positions[int(occupied[-1])]) / slab_mm)) + 1
    max_center = int(np.ceil(float(positions[-1]) / slab_mm))
    slabs = []
    for center_index in range(first_center, min(last_center, max_center) + 1):
        center_mm = float(center_index * slab_mm)
        slabs.append(
            {
                "center_mm": center_mm,
                "source_indices": _indices_for_slab(positions, center_mm, slab_mm),
            }
        )
    return slabs


def _build_bleed_slabs(mask: np.ndarray, *, spacing_z: float, slab_mm: float) -> list[dict[str, Any]]:
    occupied = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if occupied.size == 0:
        return []
    positions = _source_positions_mm(mask.shape[2], spacing_z)
    center_indices = {
        int(round(float(positions[int(idx)]) / slab_mm))
        for idx in occupied.tolist()
    }
    expanded = set(center_indices)
    for center_index in center_indices:
        expanded.add(max(0, center_index - 1))
        expanded.add(center_index + 1)
    max_center = int(np.ceil(float(positions[-1]) / slab_mm))
    return [
        {
            "center_mm": float(center_index * slab_mm),
            "source_indices": _indices_for_slab(positions, float(center_index * slab_mm), slab_mm),
            "contains_bleed": center_index in center_indices,
        }
        for center_index in sorted(idx for idx in expanded if idx <= max_center)
    ]


def _average_slab(data: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.mean(np.asarray(data[:, :, source_indices], dtype=np.float32), axis=2)


def _mask_slab(mask: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.any(np.asarray(mask[:, :, source_indices], dtype=bool), axis=2)


def _load_volume_data(image_path: Path) -> tuple[nib.Nifti1Image, np.ndarray]:
    image = nib.load(str(image_path))
    data = np.asarray(image.get_fdata(), dtype=np.float32)
    if data.ndim != 3:
        raise RuntimeError(f"Expected 3D NIfTI volume. Got shape {data.shape}")
    return image, data


def _resample_mask_to_reference(mask_path: Path, reference_image: nib.Nifti1Image) -> np.ndarray:
    source = nib.as_closest_canonical(nib.load(str(mask_path)))
    resampled = resample_from_to(
        source,
        (reference_image.shape[:3], reference_image.affine),
        order=0,
    )
    return np.asarray(resampled.get_fdata(), dtype=np.float32) > 0.5


def _render_structures_overlay_on_normalized_geometry(
    ct_slice: np.ndarray,
    structure_slices: dict[str, np.ndarray],
    *,
    locale: str,
) -> np.ndarray:
    rgb = _ct_to_rgb(np.asarray(ct_slice, dtype=np.float32).T)
    for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
        mask = structure_slices.get(mask_name)
        if mask is None or not np.asarray(mask, dtype=bool).any():
            continue
        display_mask = np.asarray(mask, dtype=bool).T
        rgb = _blend(rgb, display_mask, STRUCTURE_COLORS[idx % len(STRUCTURE_COLORS)], alpha=0.28)
    return rgb


def _draw_panel(rgb: np.ndarray, lines: list[str]) -> np.ndarray:
    image = Image.fromarray(rgb, mode="RGB")
    if image.width < 40 or image.height < 40:
        return np.asarray(image, dtype=np.uint8)
    draw = ImageDraw.Draw(image, mode="RGBA")
    title_font = _font(18)
    body_font = _font(15)
    line_heights = []
    max_width = 0
    for idx, line in enumerate(lines):
        font = title_font if idx == 0 else body_font
        bbox = draw.textbbox((0, 0), line, font=font)
        max_width = max(max_width, bbox[2] - bbox[0])
        line_heights.append((bbox[3] - bbox[1]) + 5)
    box_width = max(1, min(image.width - 20, max_width + 24))
    box_height = max(1, min(image.height - 20, sum(line_heights) + 22))
    draw.rounded_rectangle((10, 10, 10 + box_width, 10 + box_height), radius=8, fill=(0, 0, 0, 150))
    y = 18
    for idx, line in enumerate(lines):
        draw.text((20, y), line, font=title_font if idx == 0 else body_font, fill=(255, 255, 255, 255))
        y += line_heights[idx]
    return np.asarray(image, dtype=np.uint8)


def _structure_display_name(mask_name: str, locale: str) -> str:
    return translate(f"head.structures.{mask_name}", locale=locale)


def _volume_cm3(mask: np.ndarray, spacing_xyz: tuple[float, float, float]) -> float:
    voxel_volume_cm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2]) / 1000.0
    return round(float(np.count_nonzero(mask)) * voxel_volume_cm3, 3)


def _volume_rows(
    *,
    brain_mask: np.ndarray | None,
    structure_masks: dict[str, np.ndarray],
    spacing_xyz: tuple[float, float, float],
    locale: str,
) -> list[dict[str, Any]]:
    rows = []
    if brain_mask is not None:
        rows.append(
            {
                "key": "brain_total",
                "label": translate("head.volume.brain_total", locale=locale),
                "volume_cm3": _volume_cm3(brain_mask, spacing_xyz),
                "source_mask": "artifacts/total/brain.nii.gz",
            }
        )
    for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
        mask = structure_masks.get(mask_name)
        if mask is None:
            continue
        rows.append(
            {
                "key": mask_name,
                "label": _structure_display_name(mask_name, locale),
                "volume_cm3": _volume_cm3(mask, spacing_xyz),
                "color_rgb": list(STRUCTURE_COLORS[idx % len(STRUCTURE_COLORS)]),
                "source_mask": f"artifacts/brain_structures/{mask_name}.nii.gz",
            }
        )
    return rows


def _render_volume_table(rows: list[dict[str, Any]], *, locale: str) -> np.ndarray:
    width, height = 1240, 1754
    image = Image.new("RGB", (width, height), (0, 0, 0))
    draw = ImageDraw.Draw(image)
    title_font = _font(44)
    header_font = _font(30)
    row_font = _font(28)
    table_left = 64
    table_right = width - 64
    color_left = table_left
    structure_left = 188
    volume_left = 870
    volume_decimal_x = 1068
    row_height = 58
    header_height = 62
    y = 70
    title_color = (235, 245, 241)
    text_color = (238, 242, 240)
    muted_text = (205, 216, 211)
    line_color = (74, 88, 82)
    strong_line = (128, 148, 140)
    header_fill = (18, 30, 26)
    zebra_fill = (10, 17, 15)
    draw.text(
        (table_left, y),
        translate("head.volume_table.title", locale=locale),
        font=title_font,
        fill=title_color,
    )
    y += 78
    table_top = y
    table_bottom = min(height - 70, table_top + header_height + row_height * len(rows))
    draw.rectangle((table_left, table_top, table_right, table_top + header_height), fill=header_fill)
    draw.text(
        (structure_left, table_top + 15),
        translate("head.volume_table.structure", locale=locale),
        font=header_font,
        fill=muted_text,
    )
    draw.text(
        (color_left + 20, table_top + 15),
        translate("head.volume_table.color", locale=locale),
        font=header_font,
        fill=muted_text,
    )
    draw.text(
        (volume_left, table_top + 15),
        translate("head.volume_table.volume_cm3", locale=locale),
        font=header_font,
        fill=muted_text,
    )
    y = table_top + header_height
    for row_idx, row in enumerate(rows):
        if y + row_height > height - 70:
            break
        if row_idx % 2 == 1:
            draw.rectangle((table_left + 1, y + 1, table_right - 1, y + row_height - 1), fill=zebra_fill)
        volume = format_decimal(row["volume_cm3"], 1, locale=locale)
        decimal_sep = "," if "," in volume else "."
        if decimal_sep in volume:
            whole, fractional = volume.rsplit(decimal_sep, 1)
        else:
            whole, fractional = volume, ""
        whole_width = draw.textbbox((0, 0), whole, font=row_font)[2]
        text_y = y + 13
        if row.get("color_rgb") is not None:
            color = tuple(int(value) for value in row["color_rgb"])
            draw.rounded_rectangle(
                (color_left + 30, y + 16, color_left + 86, y + 42),
                radius=5,
                fill=color,
                outline=(210, 220, 216),
                width=1,
            )
        draw.text((structure_left, text_y), str(row["label"]), font=row_font, fill=text_color)
        draw.text((volume_decimal_x - whole_width, text_y), whole, font=row_font, fill=text_color)
        draw.text((volume_decimal_x, text_y), decimal_sep, font=row_font, fill=text_color)
        draw.text((volume_decimal_x + 10, text_y), fractional, font=row_font, fill=text_color)
        y += row_height
    for row_y in range(table_top + header_height, table_bottom + 1, row_height):
        draw.line((table_left, row_y, table_right, row_y), fill=line_color, width=1)
    draw.line((table_left, table_top + header_height, table_right, table_top + header_height), fill=strong_line, width=2)
    for x in (structure_left - 24, volume_left - 24):
        draw.line((x, table_top, x, table_bottom), fill=line_color, width=2)
    draw.rectangle((table_left, table_top, table_right, table_bottom), outline=strong_line, width=2)
    return np.asarray(image, dtype=np.uint8)


def _render_structures_overlay(
    ct_slice: np.ndarray,
    structure_slices: dict[str, np.ndarray],
    *,
    source_axis_codes: tuple[str, str],
    locale: str,
) -> np.ndarray:
    rgb = reorient_display_array(
        _ct_to_rgb(ct_slice),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    active_labels = []
    for idx, mask_name in enumerate(BRAIN_STRUCTURE_MASKS):
        mask = structure_slices.get(mask_name)
        if mask is None or not np.asarray(mask, dtype=bool).any():
            continue
        display_mask = reorient_display_array(
            np.asarray(mask, dtype=bool),
            source_axis_codes=source_axis_codes,
            desired_row_code="P",
            desired_col_code="L",
        )
        rgb = _blend(rgb, display_mask, STRUCTURE_COLORS[idx % len(STRUCTURE_COLORS)], alpha=0.28)
        if len(active_labels) < 5:
            active_labels.append(_structure_display_name(mask_name, locale))
    lines = [translate("head.structures_overlay.title", locale=locale)]
    lines.extend(active_labels)
    if len(active_labels) == 5:
        lines.append(translate("head.structures_overlay.more", locale=locale))
    return _draw_panel(rgb, lines)


def _render_bleed_overlay(
    ct_slice: np.ndarray,
    bleed_slice: np.ndarray,
) -> np.ndarray:
    rgb = _ct_to_rgb(np.asarray(ct_slice, dtype=np.float32).T)
    display_mask = np.asarray(bleed_slice, dtype=bool).T
    return _blend_contour(rgb, display_mask, BLEED_COLOR, alpha=0.18, outline_iterations=2)


def _bleed_mask_status(
    mask_path: Path,
    spacing_xyz: tuple[float, float, float],
    reference_shape: tuple[int, int, int],
) -> dict[str, Any]:
    if not mask_path.exists():
        status = compute_mask_status(None, spacing_xyz)
        status["task_complete"] = False
        status["segmented_hemorrhage_present"] = False
        return status

    try:
        mask = _load_mask(mask_path)
    except Exception as exc:
        return {
            "status": "read_error",
            "task_complete": False,
            "segmented_hemorrhage_present": False,
            "present": False,
            "complete": False,
            "voxel_count": 0,
            "volume_cm3": None,
            "bounds": None,
            "touches_scan_bounds": True,
            "touched_bounds": ["read_error"],
            "error": str(exc),
        }

    if tuple(mask.shape) != tuple(reference_shape):
        return {
            "status": "geometry_mismatch",
            "task_complete": False,
            "segmented_hemorrhage_present": bool(np.any(mask)),
            "present": False,
            "complete": False,
            "voxel_count": int(np.count_nonzero(mask)),
            "volume_cm3": None,
            "bounds": None,
            "touches_scan_bounds": True,
            "touched_bounds": ["geometry_mismatch"],
            "shape": [int(value) for value in mask.shape],
        }

    status = compute_mask_status(mask, spacing_xyz)
    status["task_complete"] = True
    status["segmented_hemorrhage_present"] = bool(status["voxel_count"] > 0)
    if status["status"] == "empty":
        status["complete"] = True
    return status


def _head_union_status(
    total_dir: Path,
    component_summary: dict[str, Any],
    spacing_xyz: tuple[float, float, float],
    reference_shape: tuple[int, int, int],
) -> dict[str, Any]:
    masks: list[np.ndarray] = []
    for mask_name in HEAD_COMPONENT_MASKS:
        status = component_summary["masks"].get(mask_name, {})
        if not status.get("present"):
            continue
        mask_path = total_dir / f"{mask_name}.nii.gz"
        try:
            mask = _load_mask(mask_path)
        except Exception:
            continue
        if tuple(mask.shape) == tuple(reference_shape):
            masks.append(mask)

    if not masks:
        return compute_mask_status(None, spacing_xyz)

    union = np.zeros(reference_shape, dtype=bool)
    for mask in masks:
        union |= np.asarray(mask, dtype=bool)
    return compute_mask_status(union, spacing_xyz)


def _rewrite_normalized_relpath(case_dir: Path, normalization: dict[str, Any]) -> dict[str, Any]:
    normalized_path = normalization.get("normalized_nifti")
    if normalized_path:
        normalization["normalized_nifti"] = _relpath(case_dir, Path(normalized_path))
    brain_mask_path = normalization.get("brain_mask")
    if brain_mask_path:
        normalization["brain_mask"] = _relpath(case_dir, Path(brain_mask_path))
    brain_geometry_frame = normalization.get("brain_geometry_frame")
    if isinstance(brain_geometry_frame, dict):
        crop_source = brain_geometry_frame.get("crop_source")
        if crop_source:
            crop_path = Path(str(crop_source))
            if crop_path.is_absolute() and crop_path.exists():
                brain_geometry_frame["crop_source"] = _relpath(case_dir, crop_path)
    return normalization


def main() -> int:
    args = parse_args(__doc__ or "Head complete segmentation QC job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "head_complete_qc"
    payload: dict[str, Any] = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        ct_path = resolve_canonical_nifti(args.case_id)
        total_dir = case_dir / "artifacts" / "total"
        cerebral_bleed_dir = case_dir / "artifacts" / "cerebral_bleed"
        brain_structures_dir = case_dir / "artifacts" / "brain_structures"
        normalized_path = metric_dir / "normalized_axial_head_ct.nii.gz"
        normalized_2mm_path = metric_dir / "normalized_ras_head_ct_2mm.nii.gz"
        normalized_brain_geometry_path = metric_dir / "normalized_brain_geometry_head_ct_2mm.nii.gz"

        payload["inputs"] = {
            "canonical_nifti": _relpath(case_dir, ct_path) if ct_path and ct_path.exists() else None,
            "total_dir": _relpath(case_dir, total_dir) if total_dir.exists() else None,
            "cerebral_bleed_mask": _relpath(case_dir, cerebral_bleed_dir / f"{BLEED_MASK_NAME}.nii.gz")
            if (cerebral_bleed_dir / f"{BLEED_MASK_NAME}.nii.gz").exists()
            else None,
            "brain_structures_dir": _relpath(case_dir, brain_structures_dir)
            if brain_structures_dir.exists()
            else None,
        }

        if ct_path is None or not ct_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_canonical_nifti"}
            payload["artifacts"] = {"result_json": _relpath(case_dir, result_path)}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, _ = load_ct_volume(ct_path)
        case_metadata = _load_case_metadata(args.case_id, case_dir)
        artifact_locale = _artifact_locale(job_config)
        spacing_xyz = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        reference_shape = tuple(int(value) for value in ct_img.shape[:3])
        normalization_spec = parse_normalization_spec(job_config)
        normalization = normalize_nifti_to_axial(ct_path, normalized_path, normalization_spec)
        normalization = _rewrite_normalized_relpath(case_dir, normalization)
        normalization_2mm = normalize_nifti_to_ras_isotropic(
            ct_path,
            normalized_2mm_path,
            voxel_size_mm=float(job_config.get("anatomic_normalized_spacing_mm", 2.0)),
            write_normalized_nifti=bool(job_config.get("write_anatomic_normalized_nifti", True)),
        )
        normalization_2mm = _rewrite_normalized_relpath(case_dir, normalization_2mm)
        brain_mask_path = total_dir / "brain.nii.gz"
        brain_geometry_in_plane_spacing = job_config.get("brain_geometry_normalized_in_plane_spacing_mm")
        if brain_geometry_in_plane_spacing is not None:
            brain_geometry_in_plane_spacing = (
                float(brain_geometry_in_plane_spacing[0]),
                float(brain_geometry_in_plane_spacing[1]),
            )
        normalization_brain_geometry_2mm = normalize_nifti_to_brain_mask_geometry_isotropic(
            ct_path,
            brain_mask_path,
            normalized_brain_geometry_path,
            brain_structures_dir=brain_structures_dir,
            crop_mask_path=total_dir / "skull.nii.gz",
            crop_margin_mm=float(job_config.get("brain_geometry_crop_margin_mm", 25.0)),
            voxel_size_mm=float(job_config.get("brain_geometry_normalized_spacing_mm", 1.0)),
            in_plane_spacing_mm=brain_geometry_in_plane_spacing,
            write_normalized_nifti=bool(job_config.get("write_brain_geometry_normalized_nifti", True)),
        )
        normalization_brain_geometry_2mm = _rewrite_normalized_relpath(
            case_dir,
            normalization_brain_geometry_2mm,
        )

        head_components = collect_mask_statuses(
            total_dir,
            list(HEAD_COMPONENT_MASKS),
            spacing_xyz,
            reference_shape=reference_shape,
        )
        head_union = _head_union_status(total_dir, head_components, spacing_xyz, reference_shape)
        bleed_status = _bleed_mask_status(
            cerebral_bleed_dir / f"{BLEED_MASK_NAME}.nii.gz",
            spacing_xyz,
            reference_shape,
        )
        brain_structures = collect_mask_statuses(
            brain_structures_dir,
            list(BRAIN_STRUCTURE_MASKS),
            spacing_xyz,
            reference_shape=reference_shape,
        )
        brain_mask = _load_mask(brain_mask_path) if brain_mask_path.exists() else None
        structure_masks: dict[str, np.ndarray] = {}
        for mask_name in BRAIN_STRUCTURE_MASKS:
            mask_path = brain_structures_dir / f"{mask_name}.nii.gz"
            if mask_path.exists():
                try:
                    mask = _load_mask(mask_path)
                except Exception:
                    continue
                if tuple(mask.shape) == tuple(reference_shape):
                    structure_masks[mask_name] = mask
        bleed_mask_path = cerebral_bleed_dir / f"{BLEED_MASK_NAME}.nii.gz"
        bleed_mask = _load_mask(bleed_mask_path) if bleed_mask_path.exists() else None
        has_cerebral_bleed = bool(bleed_mask is not None and np.count_nonzero(bleed_mask) > 0)
        volume_rows = _volume_rows(
            brain_mask=brain_mask,
            structure_masks=structure_masks,
            spacing_xyz=spacing_xyz,
            locale=artifact_locale,
        )

        head_complete = bool(head_components["complete"] and head_union["complete"])
        required_segmentation_complete = bool(
            head_complete
            and bleed_status.get("task_complete")
            and brain_structures["complete"]
        )
        artifacts = {
            "result_json": _relpath(case_dir, result_path),
        }
        dicom_exports: list[dict[str, str]] = []
        if normalization.get("normalized_nifti"):
            artifacts["normalized_nifti"] = normalization["normalized_nifti"]
        if normalization_2mm.get("normalized_nifti"):
            artifacts["normalized_2mm_nifti"] = normalization_2mm["normalized_nifti"]
        if normalization_brain_geometry_2mm.get("normalized_nifti"):
            artifacts["normalized_brain_geometry_2mm_nifti"] = normalization_brain_geometry_2mm["normalized_nifti"]
            if bool(job_config.get("emit_brain_geometry_dicom_series", True)):
                geometry_dicom_dir = metric_dir / "brain_geometry_ct_2mm_dicom"
                preferred_display_world_ras = (
                    normalization_brain_geometry_2mm
                    .get("brain_geometry_frame", {})
                    .get("centroid_ras_mm")
                )
                geometry_dicom_paths = create_derived_ct_series_from_nifti(
                    case_dir / normalization_brain_geometry_2mm["normalized_nifti"],
                    geometry_dicom_dir,
                    case_metadata,
                    series_description="Heimdallr Brain Geometry CT 2 mm",
                    series_number=GEOMETRY_CT_SERIES_NUMBER,
                    preferred_display_world_ras=preferred_display_world_ras,
                    slice_thickness_mm=float(job_config.get("brain_geometry_slice_thickness_mm", 2.0)),
                )
                artifacts["brain_geometry_ct_2mm_series_dir"] = _relpath(case_dir, geometry_dicom_dir)
                for dicom_path in geometry_dicom_paths:
                    dicom_exports.append({"path": _relpath(case_dir, dicom_path), "kind": "derived_ct"})

        secondary_capture_options = secondary_capture_options_from_job_config(job_config)
        emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", True))
        if emit_dicom and volume_rows:
            volume_table_dir = metric_dir / "volume_table_dicom"
            volume_table_dir.mkdir(parents=True, exist_ok=True)
            volume_table_path = volume_table_dir / "volume_table_0001.dcm"
            volume_table_options = dict(secondary_capture_options)
            volume_table_options["max_dimension"] = job_config.get(
                "volume_table_secondary_capture_max_dimension"
            )
            create_secondary_capture_from_rgb(
                _render_volume_table(volume_rows, locale=artifact_locale),
                volume_table_path,
                case_metadata,
                series_instance_uid=generate_uid(),
                series_description=translate("head.volume_table.series_description", locale=artifact_locale),
                series_number=VOLUME_TABLE_SERIES_NUMBER,
                instance_number=1,
                derivation_description=translate("head.volume_table.derivation_description", locale=artifact_locale),
                **volume_table_options,
            )
            artifacts["volume_table_dicom"] = _relpath(case_dir, volume_table_path)
            dicom_exports.append({"path": _relpath(case_dir, volume_table_path), "kind": "secondary_capture"})

        if emit_dicom and structure_masks:
            structures_dir = metric_dir / "brain_structures_dicom"
            structures_dir.mkdir(parents=True, exist_ok=True)
            for stale_path in structures_dir.glob("brain_structures_*.dcm"):
                stale_path.unlink()
            normalized_geometry_image, normalized_geometry_data = _load_volume_data(
                case_dir / normalization_brain_geometry_2mm["normalized_nifti"]
            )
            normalized_geometry_spacing = tuple(
                float(value) for value in normalized_geometry_image.header.get_zooms()[:3]
            )
            normalized_structure_masks: dict[str, np.ndarray] = {}
            for mask_name in structure_masks:
                mask_path = brain_structures_dir / f"{mask_name}.nii.gz"
                if mask_path.exists():
                    normalized_structure_masks[mask_name] = _resample_mask_to_reference(
                        mask_path,
                        normalized_geometry_image,
                    )
            structure_union = np.zeros(normalized_geometry_data.shape[:3], dtype=bool)
            for mask in normalized_structure_masks.values():
                structure_union |= np.asarray(mask, dtype=bool)
            structure_slabs = _build_slabs(
                structure_union,
                spacing_z=float(normalized_geometry_spacing[2]),
                slab_mm=float(job_config.get("overlay_slice_thickness_mm", OVERLAY_SLICE_THICKNESS_MM)),
            )
            structure_series_uid = generate_uid()
            for output_idx, slab in enumerate(structure_slabs, start=1):
                source_indices = slab["source_indices"]
                rgb = _render_structures_overlay_on_normalized_geometry(
                    _average_slab(normalized_geometry_data, source_indices),
                    {
                        name: _mask_slab(mask, source_indices)
                        for name, mask in normalized_structure_masks.items()
                    },
                    locale=artifact_locale,
                )
                dicom_path = structures_dir / f"brain_structures_{output_idx:04d}.dcm"
                create_secondary_capture_from_rgb(
                    rgb,
                    dicom_path,
                    case_metadata,
                    series_instance_uid=structure_series_uid,
                    series_description=translate("head.structures_overlay.series_description", locale=artifact_locale),
                    series_number=STRUCTURE_SERIES_NUMBER,
                    instance_number=output_idx,
                    derivation_description=translate("head.structures_overlay.derivation_description", locale=artifact_locale),
                    **secondary_capture_options,
                )
                dicom_exports.append({"path": _relpath(case_dir, dicom_path), "kind": "secondary_capture"})
            artifacts["brain_structures_overlay_series_dir"] = _relpath(case_dir, structures_dir)

        bleed_exported_slabs: list[dict[str, Any]] = []
        if emit_dicom and has_cerebral_bleed and bleed_mask is not None:
            bleed_dir = metric_dir / "cerebral_bleed_dicom"
            bleed_dir.mkdir(parents=True, exist_ok=True)
            for stale_path in bleed_dir.glob("cerebral_bleed_*.dcm"):
                stale_path.unlink()
            normalized_geometry_image, normalized_geometry_data = _load_volume_data(
                case_dir / normalization_brain_geometry_2mm["normalized_nifti"]
            )
            normalized_geometry_spacing = tuple(
                float(value) for value in normalized_geometry_image.header.get_zooms()[:3]
            )
            normalized_bleed_mask = _resample_mask_to_reference(
                bleed_mask_path,
                normalized_geometry_image,
            )
            bleed_exported_slabs = _build_bleed_slabs(
                normalized_bleed_mask,
                spacing_z=float(normalized_geometry_spacing[2]),
                slab_mm=float(
                    job_config.get(
                        "bleed_overlay_slice_thickness_mm",
                        BLEED_OVERLAY_SLICE_THICKNESS_MM,
                    )
                ),
            )
            bleed_series_uid = generate_uid()
            for output_idx, slab in enumerate(bleed_exported_slabs, start=1):
                source_indices = slab["source_indices"]
                rgb = _render_bleed_overlay(
                    _average_slab(normalized_geometry_data, source_indices),
                    _mask_slab(normalized_bleed_mask, source_indices),
                )
                dicom_path = bleed_dir / f"cerebral_bleed_{output_idx:04d}.dcm"
                create_secondary_capture_from_rgb(
                    rgb,
                    dicom_path,
                    case_metadata,
                    series_instance_uid=bleed_series_uid,
                    series_description=translate("head.bleed_overlay.series_description", locale=artifact_locale),
                    series_number=BLEED_SERIES_NUMBER,
                    instance_number=output_idx,
                    derivation_description=translate("head.bleed_overlay.derivation_description", locale=artifact_locale),
                    **secondary_capture_options,
                )
                dicom_exports.append({"path": _relpath(case_dir, dicom_path), "kind": "secondary_capture"})
            artifacts["cerebral_bleed_overlay_series_dir"] = _relpath(case_dir, bleed_dir)

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete" if required_segmentation_complete else "incomplete_head_segmentation",
                "head_complete_without_truncation": head_complete,
                "required_segmentation_complete": required_segmentation_complete,
                "source_spacing_mm": {
                    "x": spacing_xyz[0],
                    "y": spacing_xyz[1],
                    "z": spacing_xyz[2],
                },
                "source_shape": [int(value) for value in reference_shape],
                "head_definition": {
                    "components": list(HEAD_COMPONENT_MASKS),
                    "rule": "head is complete when skull and brain masks are present, non-empty, and do not touch scan bounds",
                },
                "head_components": head_components,
                "head_union": head_union,
                "cerebral_bleed": {
                    "mask_name": BLEED_MASK_NAME,
                    "mask": bleed_status,
                    "segmented_hemorrhage_volume_cm3": bleed_status.get("volume_cm3"),
                    "segmented_hemorrhage_present": bool(
                        bleed_status.get("segmented_hemorrhage_present")
                    ),
                    "has_cerebral_bleed": has_cerebral_bleed,
                    "notification_bool": has_cerebral_bleed,
                    "overlay_exported_slabs": bleed_exported_slabs,
                },
                "brain_structures": brain_structures,
                "brain_structure_volumes": {
                    "locale": artifact_locale,
                    "rows": volume_rows,
                },
                "normalization": normalization,
                "normalization_2mm": normalization_2mm,
                "normalization_brain_geometry_2mm": normalization_brain_geometry_2mm,
            },
            "artifacts": artifacts,
            "dicom_exports": dicom_exports,
        }
        write_payload(result_path, payload)
    except Exception as exc:
        payload["error"] = str(exc)
        print(json.dumps(payload, indent=2))
        return 1

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
