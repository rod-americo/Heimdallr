#!/usr/bin/env python3
"""Quantify parenchymal organs and render a 5 mm overlay DICOM series."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, ImageDraw, ImageFont
from pydicom.uid import generate_uid
from scipy.ndimage import binary_erosion, zoom

from heimdallr.metrics.jobs._bone_job_common import (
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    read_json,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.metrics.jobs._parenchymal_overlay_text import (
    build_overlay_text,
    resolve_artifact_locale,
)
from heimdallr.shared.paths import study_metadata_json


TARGET_SLICE_THICKNESS_MM = 5.0
SERIES_DESCRIPTION = "Heimdallr Parenchymal Organ Overlay 5 mm"
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


def _load_case_metadata(case_id: str, case_dir: Path) -> dict[str, Any]:
    metadata_json = read_json(study_metadata_json(case_id))
    id_json = read_json(case_dir / "metadata" / "id.json")
    merged = {}
    merged.update(id_json)
    merged.update(metadata_json)
    return merged


def _resample_along_z(volume: np.ndarray, z_scale: float, *, order: int) -> np.ndarray:
    if volume.ndim != 3:
        raise ValueError(f"Expected 3D volume. Got {volume.shape}")
    if z_scale <= 0:
        raise ValueError(f"z_scale must be positive. Got {z_scale}")
    return zoom(volume, zoom=(1.0, 1.0, z_scale), order=order)


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

    voxel_volume_cm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2]) / 1000.0
    observed_volume_cm3 = round(voxel_count * voxel_volume_cm3, 3)
    hu_values = ct_data[mask_bool]
    hu_mean = round(float(np.mean(hu_values)), 2) if hu_values.size else None
    hu_std = round(float(np.std(hu_values)), 2) if hu_values.size else None
    complete = mask_complete(mask_bool)

    return {
        "organ_key": organ_key,
        "organ_label": organ_label,
        "analysis_status": "complete" if complete else "incomplete",
        "complete": bool(complete),
        "voxel_count": voxel_count,
        "observed_volume_cm3": observed_volume_cm3,
        "volume_cm3": observed_volume_cm3 if complete else None,
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


def _legend_height(line_count: int) -> int:
    return 44 + (line_count * 20)


def _render_slice_rgb(
    ct_slice: np.ndarray,
    masks_for_slice: list[tuple[np.ndarray, tuple[int, int, int]]],
    summary_lines: list[str],
) -> np.ndarray:
    rgb = _ct_to_rgb(ct_slice)
    rgb = np.rot90(rgb, axes=(0, 1))
    for organ_mask, color in masks_for_slice:
        rotated_mask = np.rot90(np.asarray(organ_mask, dtype=bool))
        rgb = _blend_mask(rgb, rotated_mask, color, alpha=0.33)

    image = Image.fromarray(rgb, mode="RGB")
    draw = ImageDraw.Draw(image, mode="RGBA")
    font = ImageFont.load_default()

    max_width = 0
    for line in summary_lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        max_width = max(max_width, bbox[2] - bbox[0])
    box_width = min(image.width - 20, max_width + 24)
    box_height = _legend_height(len(summary_lines))
    draw.rounded_rectangle(
        (10, 10, 10 + box_width, 10 + box_height),
        radius=8,
        fill=(0, 0, 0, 150),
    )

    y = 18
    for idx, line in enumerate(summary_lines):
        fill = (255, 255, 255, 255) if idx == 0 else (235, 235, 235, 255)
        draw.text((20, y), line, font=font, fill=fill)
        y += 20

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

        z_scale = float(spacing_xyz[2]) / TARGET_SLICE_THICKNESS_MM
        resampled_ct = _resample_along_z(ct_data, z_scale, order=1)
        resampled_masks = {
            organ_key: (
                _resample_along_z(mask.astype(np.float32), z_scale, order=0) > 0.5
                if mask is not None
                else None
            )
            for organ_key, mask in organ_masks.items()
        }
        union_mask = np.zeros(resampled_ct.shape, dtype=bool)
        for mask in resampled_masks.values():
            if mask is not None:
                union_mask |= np.asarray(mask, dtype=bool)

        export_indices = np.where(union_mask.sum(axis=(0, 1)) > 0)[0].tolist()
        if not export_indices:
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "empty_resampled_overlay",
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
            for output_idx, slice_idx in enumerate(export_indices, start=1):
                masks_for_slice = []
                for organ_key, _organ_label, _filename, color in ORGAN_DEFINITIONS:
                    mask = resampled_masks[organ_key]
                    if mask is None:
                        continue
                    slice_mask = np.asarray(mask[:, :, slice_idx], dtype=bool)
                    if slice_mask.any():
                        masks_for_slice.append((slice_mask, color))

                if not masks_for_slice:
                    continue

                summary_lines = build_overlay_text(
                    organ_measurements=organ_measurements,
                    locale=artifact_locale,
                )
                rgb = _render_slice_rgb(resampled_ct[:, :, slice_idx], masks_for_slice, summary_lines)
                if emit_dicom:
                    dicom_path = dicom_dir / f"overlay_{output_idx:04d}.dcm"
                    create_secondary_capture_from_rgb(
                        rgb,
                        dicom_path,
                        case_metadata,
                        series_instance_uid=series_instance_uid,
                        series_description=SERIES_DESCRIPTION,
                        series_number=SERIES_NUMBER,
                        instance_number=output_idx,
                        derivation_description=(
                            "5 mm axial reconstruction with parenchymal-organ overlays "
                            "(liver, spleen, pancreas, right kidney, left kidney)"
                        ),
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
                "resampled_slice_count": int(resampled_ct.shape[2]),
                "exported_slice_count": len(dicom_exports),
                "exported_slice_indices": [int(idx) for idx in export_indices],
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
