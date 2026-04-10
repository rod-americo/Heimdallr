#!/usr/bin/env python3
"""Quantify parenchymal organs and render a 5 mm overlay DICOM series."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from pydicom.uid import generate_uid
from scipy.ndimage import binary_erosion

from heimdallr.metrics.jobs._bone_job_common import (
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    plane_source_axis_codes,
    read_json,
    reorient_display_array,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.metrics.jobs._parenchymal_overlay_text import (
    build_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_metadata_json


TARGET_SLICE_THICKNESS_MM = 5.0
SERIES_NUMBER = 9105
WINDOW_MIN = -160.0
WINDOW_MAX = 240.0
ORGAN_DEFINITIONS = [
    ("liver", "Liver", "liver.nii.gz", (255, 140, 66)),
    ("spleen", "Spleen", "spleen.nii.gz", (53, 197, 240)),
    ("pancreas", "Pancreas", "pancreas.nii.gz", (214, 84, 255)),
    ("kidney_right", "Right kidney", "kidney_right.nii.gz", (91, 214, 114)),
    ("kidney_left", "Left kidney", "kidney_left.nii.gz", (255, 214, 64)),
]


def _load_overlay_font(size: int) -> ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
        "/Library/Fonts/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if not path.exists():
            continue
        try:
            return ImageFont.truetype(str(path), size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _load_case_metadata(case_id: str, case_dir: Path) -> dict[str, Any]:
    metadata_json = read_json(study_metadata_json(case_id))
    id_json = read_json(case_dir / "metadata" / "id.json")
    merged = {}
    merged.update(id_json)
    merged.update(metadata_json)
    return merged


def _compute_mask_measurement(
    organ_key: str,
    organ_label: str,
    organ_mask: np.ndarray | None,
    ct_data: np.ndarray,
    spacing_xyz: tuple[float, float, float],
) -> dict[str, Any]:
    if organ_mask is None:
        return {
            "organ_key": organ_key,
            "organ_label": organ_label,
            "analysis_status": "missing",
            "complete": False,
            "voxel_count": 0,
            "observed_volume_cm3": None,
            "volume_cm3": None,
            "hu_mean": None,
            "hu_std": None,
        }

    mask_bool = np.asarray(organ_mask, dtype=bool)
    voxel_count = int(mask_bool.sum())
    if voxel_count == 0:
        return {
            "organ_key": organ_key,
            "organ_label": organ_label,
            "analysis_status": "empty",
            "complete": False,
            "voxel_count": 0,
            "observed_volume_cm3": 0.0,
            "volume_cm3": None,
            "hu_mean": None,
            "hu_std": None,
        }

    hu_values = ct_data[mask_bool]
    hu_mean = round(float(np.mean(hu_values)), 2) if hu_values.size else None
    hu_std = round(float(np.std(hu_values)), 2) if hu_values.size else None
    complete = mask_complete(mask_bool)
    voxel_volume_cm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2]) / 1000.0
    observed_volume_cm3 = round(voxel_count * voxel_volume_cm3, 3) if complete else None
    occupied_indices = np.where(mask_bool.sum(axis=(0, 1)) > 0)[0]
    axial_slice_extent = None
    if occupied_indices.size > 0:
        axial_slice_extent = {
            "start": int(occupied_indices[0]),
            "end": int(occupied_indices[-1]),
        }

    return {
        "organ_key": organ_key,
        "organ_label": organ_label,
        "analysis_status": "complete" if complete else "incomplete",
        "complete": bool(complete),
        "truncated_at_scan_bounds": not bool(complete),
        "axial_slice_extent": axial_slice_extent,
        "voxel_count": voxel_count,
        "observed_volume_cm3": observed_volume_cm3,
        "volume_cm3": observed_volume_cm3,
        "hu_mean": hu_mean,
        "hu_std": hu_std,
    }


def _ct_to_rgb(ct_slice: np.ndarray) -> np.ndarray:
    ct_clipped = np.clip(np.asarray(ct_slice, dtype=np.float32), WINDOW_MIN, WINDOW_MAX)
    scaled = ((ct_clipped - WINDOW_MIN) / (WINDOW_MAX - WINDOW_MIN) * 255.0).astype(np.uint8)
    return np.repeat(scaled[..., None], 3, axis=2)


def _outline_mask(mask_2d: np.ndarray) -> np.ndarray:
    mask_bool = np.asarray(mask_2d, dtype=bool)
    if not mask_bool.any():
        return mask_bool
    return mask_bool & ~binary_erosion(mask_bool, iterations=1)


def _blend_mask(rgb: np.ndarray, mask_2d: np.ndarray, color: tuple[int, int, int], *, alpha: float) -> np.ndarray:
    out = np.asarray(rgb, dtype=np.float32).copy()
    mask_bool = np.asarray(mask_2d, dtype=bool)
    if not mask_bool.any():
        return out.astype(np.uint8)
    color_arr = np.asarray(color, dtype=np.float32)
    out[mask_bool] = ((1.0 - alpha) * out[mask_bool]) + (alpha * color_arr)
    outline = _outline_mask(mask_bool)
    out[outline] = color_arr
    return np.clip(out, 0, 255).astype(np.uint8)


def _source_slice_positions_mm(z_size: int, spacing_z: float) -> np.ndarray:
    return np.arange(int(z_size), dtype=np.float32) * float(spacing_z)


def _select_slab_source_indices(
    source_positions_mm: np.ndarray,
    *,
    center_mm: float,
    slab_thickness_mm: float,
) -> np.ndarray:
    half_thickness = float(slab_thickness_mm) / 2.0
    selected = np.where(
        (source_positions_mm >= (center_mm - half_thickness))
        & (source_positions_mm <= (center_mm + half_thickness))
    )[0]
    if selected.size > 0:
        return selected
    nearest = int(np.argmin(np.abs(source_positions_mm - center_mm)))
    return np.asarray([nearest], dtype=np.int32)


def _build_export_slabs(
    union_mask: np.ndarray,
    *,
    spacing_z: float,
    slab_thickness_mm: float,
) -> list[dict[str, Any]]:
    occupied_indices = np.where(np.asarray(union_mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if occupied_indices.size == 0:
        return []

    source_positions_mm = _source_slice_positions_mm(union_mask.shape[2], spacing_z)
    step_mm = float(slab_thickness_mm)
    max_center_index = int(np.ceil(float(source_positions_mm[-1]) / step_mm))
    occupied_start_mm = float(source_positions_mm[int(occupied_indices[0])])
    occupied_end_mm = float(source_positions_mm[int(occupied_indices[-1])])

    center_index_start = max(0, int(np.floor(occupied_start_mm / step_mm)) - 1)
    center_index_end = min(max_center_index, int(np.ceil(occupied_end_mm / step_mm)) + 1)

    slabs: list[dict[str, Any]] = []
    for center_index in range(center_index_start, center_index_end + 1):
        center_mm = float(center_index * step_mm)
        source_indices = _select_slab_source_indices(
            source_positions_mm,
            center_mm=center_mm,
            slab_thickness_mm=slab_thickness_mm,
        )
        slabs.append(
            {
                "center_mm": center_mm,
                "source_indices": [int(idx) for idx in source_indices.tolist()],
            }
        )
    return slabs


def _average_ct_slab(ct_data: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.mean(np.asarray(ct_data[:, :, source_indices], dtype=np.float32), axis=2)


def _mask_slab(mask_data: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.any(np.asarray(mask_data[:, :, source_indices], dtype=bool), axis=2)


def _render_slice_rgb(
    ct_slice: np.ndarray,
    masks_for_slice: list[tuple[np.ndarray, tuple[int, int, int]]],
    summary_lines: list[str],
    *,
    source_axis_codes: tuple[str, str],
) -> np.ndarray:
    rgb = reorient_display_array(
        _ct_to_rgb(ct_slice),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="R",
    )
    for organ_mask, color in masks_for_slice:
        display_mask = reorient_display_array(
            np.asarray(organ_mask, dtype=bool),
            source_axis_codes=source_axis_codes,
            desired_row_code="P",
            desired_col_code="R",
        )
        rgb = _blend_mask(rgb, display_mask, color, alpha=0.33)

    image = Image.fromarray(rgb, mode="RGB")
    draw = ImageDraw.Draw(image, mode="RGBA")
    title_font = _load_overlay_font(size=18)
    body_font = _load_overlay_font(size=16)

    line_heights: list[int] = []
    max_width = 0
    for idx, line in enumerate(summary_lines):
        font = title_font if idx == 0 else body_font
        bbox = draw.textbbox((0, 0), line, font=font)
        max_width = max(max_width, bbox[2] - bbox[0])
        line_heights.append((bbox[3] - bbox[1]) + (8 if idx == 0 else 6))
    available_width = max(1, image.width - 20)
    available_height = max(1, image.height - 20)
    box_width = min(available_width, max_width + 24)
    box_height = min(available_height, 18 + sum(line_heights) + 8)
    draw.rounded_rectangle(
        (10, 10, 10 + box_width, 10 + box_height),
        radius=8,
        fill=(0, 0, 0, 150),
    )

    y = 18
    for idx, line in enumerate(summary_lines):
        font = title_font if idx == 0 else body_font
        fill = (255, 255, 255, 255) if idx == 0 else (235, 235, 235, 255)
        draw.text((20, y), line, font=font, fill=fill)
        y += line_heights[idx]

    return np.asarray(image, dtype=np.uint8)


def main() -> int:
    args = parse_args(__doc__ or "Parenchymal organ volumetry job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "parenchymal_organ_volumetry"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        dicom_dir = metric_dir / "dicom"
        dicom_dir.mkdir(parents=True, exist_ok=True)

        ct_path = resolve_canonical_nifti(args.case_id)
        total_dir = case_dir / "artifacts" / "total"
        organ_paths = {organ_key: total_dir / filename for organ_key, _label, filename, _color in ORGAN_DEFINITIONS}
        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "organ_masks": {
                organ_key: str(path.relative_to(case_dir)) if path.exists() else None
                for organ_key, path in organ_paths.items()
            },
            "target_slice_thickness_mm": TARGET_SLICE_THICKNESS_MM,
        }

        if ct_path is None or not ct_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_canonical_nifti"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        case_metadata = _load_case_metadata(args.case_id, case_dir)
        ct_img, ct_data = load_ct_volume(ct_path)
        spacing_xyz = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        axial_source_codes = plane_source_axis_codes(ct_img.affine, "z")
        organ_masks: dict[str, np.ndarray | None] = {}
        organ_measurements: dict[str, dict[str, Any]] = {}

        for organ_key, organ_label, _filename, _color in ORGAN_DEFINITIONS:
            mask_path = organ_paths[organ_key]
            organ_mask = None
            if mask_path.exists():
                _, loaded_mask = load_nifti_mask(mask_path)
                if loaded_mask.shape == ct_data.shape:
                    organ_mask = loaded_mask
            organ_masks[organ_key] = organ_mask
            organ_measurements[organ_key] = _compute_mask_measurement(
                organ_key,
                organ_label,
                organ_mask,
                ct_data,
                spacing_xyz,
            )

        available_masks = [mask for mask in organ_masks.values() if mask is not None and np.any(mask)]
        if not available_masks:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_organs", "organs": organ_measurements}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        union_mask = np.zeros(ct_data.shape, dtype=bool)
        for mask in organ_masks.values():
            if mask is not None:
                union_mask |= np.asarray(mask, dtype=bool)

        export_slabs = _build_export_slabs(
            union_mask,
            spacing_z=float(spacing_xyz[2]),
            slab_thickness_mm=TARGET_SLICE_THICKNESS_MM,
        )
        if not export_slabs:
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "empty_overlay",
                "organs": organ_measurements,
            }
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        artifacts = {
            "result_json": str(result_path.relative_to(case_dir)),
        }
        dicom_exports: list[dict[str, str]] = []

        if job_config.get("generate_overlay", True):
            emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", True))
            artifact_locale = resolve_artifact_locale(job_config)
            series_instance_uid = generate_uid()
            for output_idx, slab in enumerate(export_slabs, start=1):
                source_indices = slab["source_indices"]
                masks_for_slice = []
                for organ_key, _organ_label, _filename, color in ORGAN_DEFINITIONS:
                    mask = organ_masks[organ_key]
                    if mask is None:
                        continue
                    slice_mask = _mask_slab(mask, source_indices)
                    if slice_mask.any():
                        masks_for_slice.append((slice_mask, color))

                summary_lines = build_overlay_text(
                    organ_measurements=organ_measurements,
                    locale=artifact_locale,
                )
                rgb = _render_slice_rgb(
                    _average_ct_slab(ct_data, source_indices),
                    masks_for_slice,
                    summary_lines,
                    source_axis_codes=axial_source_codes,
                )
                if emit_dicom:
                    dicom_path = dicom_dir / f"overlay_{output_idx:04d}.dcm"
                    create_secondary_capture_from_rgb(
                        rgb,
                        dicom_path,
                        case_metadata,
                        series_instance_uid=series_instance_uid,
                        series_description=series_description(artifact_locale),
                        series_number=SERIES_NUMBER,
                        instance_number=output_idx,
                        derivation_description=derivation_description(artifact_locale),
                    )
                    dicom_exports.append(
                        {
                            "path": str(dicom_path.relative_to(case_dir)),
                            "kind": "secondary_capture",
                        }
                    )
            if emit_dicom and dicom_exports:
                artifacts["overlay_series_dir"] = str(dicom_dir.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "target_slice_thickness_mm": TARGET_SLICE_THICKNESS_MM,
                "source_spacing_mm": {
                    "x": spacing_xyz[0],
                    "y": spacing_xyz[1],
                    "z": spacing_xyz[2],
                },
                "reconstruction_mode": "slab_average",
                "source_slice_count": int(ct_data.shape[2]),
                "exported_slice_count": len(dicom_exports),
                "exported_slabs": export_slabs,
                "organs": organ_measurements,
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
