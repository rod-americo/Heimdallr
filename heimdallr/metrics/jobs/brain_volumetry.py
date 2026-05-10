#!/usr/bin/env python3
"""Quantify brain volume and render a 5 mm axial overlay DICOM series."""

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
from heimdallr.metrics.jobs._brain_volumetry_overlay_text import (
    build_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.shared.paths import study_metadata_json


TARGET_SLICE_THICKNESS_MM = 5.0
SERIES_NUMBER = 9106
WINDOW_MIN = 0.0
WINDOW_MAX = 80.0
BRAIN_MASK_FILENAME = "brain.nii.gz"
BRAIN_OVERLAY_COLOR = (95, 190, 255)


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


def _compute_brain_measurement(
    brain_mask: np.ndarray | None,
    spacing_xyz: tuple[float, float, float],
) -> dict[str, Any]:
    if brain_mask is None:
        return {
            "analysis_status": "missing",
            "complete": False,
            "voxel_count": 0,
            "observed_volume_cm3": None,
            "volume_cm3": None,
        }

    mask_bool = np.asarray(brain_mask, dtype=bool)
    voxel_count = int(mask_bool.sum())
    if voxel_count == 0:
        return {
            "analysis_status": "empty",
            "complete": False,
            "voxel_count": 0,
            "observed_volume_cm3": 0.0,
            "volume_cm3": None,
        }

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
        "analysis_status": "complete" if complete else "incomplete",
        "complete": bool(complete),
        "truncated_at_scan_bounds": not bool(complete),
        "axial_slice_extent": axial_slice_extent,
        "voxel_count": voxel_count,
        "observed_volume_cm3": observed_volume_cm3,
        "volume_cm3": observed_volume_cm3,
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
    brain_mask_slice: np.ndarray,
    summary_lines: list[str],
    *,
    source_axis_codes: tuple[str, str],
) -> np.ndarray:
    rgb = reorient_display_array(
        _ct_to_rgb(ct_slice),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    display_mask = reorient_display_array(
        np.asarray(brain_mask_slice, dtype=bool),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    rgb = _blend_mask(rgb, display_mask, BRAIN_OVERLAY_COLOR, alpha=0.34)

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
    args = parse_args(__doc__ or "Brain volumetry job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "brain_volumetry"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        dicom_dir = metric_dir / "dicom"
        dicom_dir.mkdir(parents=True, exist_ok=True)

        ct_path = resolve_canonical_nifti(args.case_id)
        brain_path = case_dir / "artifacts" / "total" / BRAIN_MASK_FILENAME
        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "brain_mask": str(brain_path.relative_to(case_dir)) if brain_path.exists() else None,
            "target_slice_thickness_mm": TARGET_SLICE_THICKNESS_MM,
        }

        if ct_path is None or not ct_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_canonical_nifti"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        if not brain_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "missing_brain_mask",
                "brain": _compute_brain_measurement(None, (1.0, 1.0, 1.0)),
            }
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        case_metadata = _load_case_metadata(args.case_id, case_dir)
        ct_img, ct_data = load_ct_volume(ct_path)
        spacing_xyz = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        axial_source_codes = plane_source_axis_codes(ct_img.affine, "z")
        _brain_img, brain_mask = load_nifti_mask(brain_path)

        if brain_mask.shape != ct_data.shape:
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "geometry_mismatch",
                "ct_shape": [int(value) for value in ct_data.shape],
                "brain_mask_shape": [int(value) for value in brain_mask.shape],
            }
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        brain_measurement = _compute_brain_measurement(brain_mask, spacing_xyz)
        if not np.asarray(brain_mask, dtype=bool).any():
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "empty_brain_mask",
                "brain": brain_measurement,
            }
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        export_slabs = _build_export_slabs(
            brain_mask,
            spacing_z=float(spacing_xyz[2]),
            slab_thickness_mm=TARGET_SLICE_THICKNESS_MM,
        )
        if not export_slabs:
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "empty_overlay",
                "brain": brain_measurement,
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
            summary_lines = build_overlay_text(
                measurement=brain_measurement,
                locale=artifact_locale,
            )
            series_instance_uid = generate_uid()
            for output_idx, slab in enumerate(export_slabs, start=1):
                source_indices = slab["source_indices"]
                rgb = _render_slice_rgb(
                    _average_ct_slab(ct_data, source_indices),
                    _mask_slab(brain_mask, source_indices),
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
                "display_window_hu": {
                    "min": WINDOW_MIN,
                    "max": WINDOW_MAX,
                },
                "reconstruction_mode": "slab_average",
                "source_slice_count": int(ct_data.shape[2]),
                "exported_slice_count": len(dicom_exports),
                "exported_slabs": export_slabs,
                "brain": brain_measurement,
                "brain_volume_cm3": brain_measurement.get("volume_cm3"),
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
