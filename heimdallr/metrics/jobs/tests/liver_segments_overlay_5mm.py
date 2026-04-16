#!/usr/bin/env python3
"""Render a 5 mm DICOM overlay series for experimental hepatic segments."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from pydicom.uid import generate_uid

from heimdallr.metrics.jobs._bone_job_common import (
    load_job_config,
    load_nifti_mask,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.metrics.jobs.parenchymal_organ_volumetry import (
    TARGET_SLICE_THICKNESS_MM,
    _average_ct_slab,
    _build_export_slabs,
    _load_case_metadata,
    _mask_slab,
    _render_slice_rgb,
    load_ct_volume,
    plane_source_axis_codes,
)
from heimdallr.metrics.jobs.tests._liver_segments_overlay_text import (
    build_overlay_text,
    resolve_artifact_locale,
)


SERIES_NUMBER = 9110
SEGMENT_DEFINITIONS = [
    ("liver_segment_1", (255, 89, 94)),
    ("liver_segment_2", (255, 202, 58)),
    ("liver_segment_3", (138, 201, 38)),
    ("liver_segment_4", (25, 130, 196)),
    ("liver_segment_5", (106, 76, 147)),
    ("liver_segment_6", (255, 146, 76)),
    ("liver_segment_7", (76, 201, 240)),
    ("liver_segment_8", (233, 30, 99)),
]


def _compute_mask_measurement(mask_data: np.ndarray, spacing_xyz: tuple[float, float, float]) -> dict[str, Any]:
    mask_bool = np.asarray(mask_data, dtype=bool)
    voxel_count = int(mask_bool.sum())
    if voxel_count == 0:
        return {
            "voxel_count": 0,
            "volume_cm3": None,
        }
    voxel_volume_cm3 = float(spacing_xyz[0] * spacing_xyz[1] * spacing_xyz[2]) / 1000.0
    return {
        "voxel_count": voxel_count,
        "volume_cm3": round(voxel_count * voxel_volume_cm3, 0),
    }


def main() -> int:
    args = parse_args(__doc__ or "Liver segments overlay 5 mm job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "liver_segments_overlay_5mm"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        dicom_dir = metric_dir / "dicom"
        dicom_dir.mkdir(parents=True, exist_ok=True)

        ct_path = resolve_canonical_nifti(args.case_id)
        segment_dir = case_dir / "artifacts" / "tests" / "liver_segments"
        segment_paths = {
            segment_key: segment_dir / f"{segment_key}.nii.gz"
            for segment_key, _color in SEGMENT_DEFINITIONS
        }
        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "segment_masks": {
                segment_key: str(path.relative_to(case_dir)) if path.exists() else None
                for segment_key, path in segment_paths.items()
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

        ct_img, ct_data = load_ct_volume(ct_path)
        spacing_xyz = tuple(float(v) for v in ct_img.header.get_zooms()[:3])
        axial_source_codes = plane_source_axis_codes(ct_img.affine, "z")
        segment_measurements: dict[str, dict[str, Any]] = {}
        segment_masks: dict[str, np.ndarray] = {}
        union_mask = np.zeros(ct_data.shape, dtype=bool)

        for segment_key, _color in SEGMENT_DEFINITIONS:
            path = segment_paths[segment_key]
            if not path.exists():
                continue
            _img, mask_data = load_nifti_mask(path)
            if mask_data.shape != ct_data.shape:
                continue
            segment_masks[segment_key] = mask_data
            union_mask |= np.asarray(mask_data, dtype=bool)
            segment_measurements[segment_key] = _compute_mask_measurement(mask_data, spacing_xyz)

        if not segment_masks or not union_mask.any():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_segments", "segments": segment_measurements}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        export_slabs = _build_export_slabs(
            union_mask,
            spacing_z=float(spacing_xyz[2]),
            slab_thickness_mm=TARGET_SLICE_THICKNESS_MM,
        )
        case_metadata = _load_case_metadata(args.case_id, case_dir)
        locale = resolve_artifact_locale(job_config)
        summary_lines = build_overlay_text(
            segment_measurements=segment_measurements,
            locale=locale,
        )
        series_instance_uid = generate_uid()
        dicom_exports: list[dict[str, str]] = []

        for output_idx, slab in enumerate(export_slabs, start=1):
            source_indices = slab["source_indices"]
            masks_for_slice = []
            for segment_key, color in SEGMENT_DEFINITIONS:
                mask_data = segment_masks.get(segment_key)
                if mask_data is None:
                    continue
                slice_mask = _mask_slab(mask_data, source_indices)
                if slice_mask.any():
                    masks_for_slice.append((slice_mask, color))

            rgb = _render_slice_rgb(
                _average_ct_slab(ct_data, source_indices),
                masks_for_slice,
                summary_lines,
                source_axis_codes=axial_source_codes,
            )
            dicom_path = dicom_dir / f"overlay_{output_idx:04d}.dcm"
            create_secondary_capture_from_rgb(
                rgb,
                dicom_path,
                case_metadata,
                series_instance_uid=series_instance_uid,
                series_description="Liver segments 5mm",
                series_number=SERIES_NUMBER,
                instance_number=output_idx,
                derivation_description="5 mm slab average with hepatic segment overlays",
            )
            dicom_exports.append(
                {
                    "path": str(dicom_path.relative_to(case_dir)),
                    "kind": "secondary_capture",
                }
            )

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
                "source_slice_count": int(ct_data.shape[2]),
                "exported_slice_count": len(dicom_exports),
                "exported_slabs": export_slabs,
                "segments": segment_measurements,
            },
            "artifacts": {
                "result_json": str(result_path.relative_to(case_dir)),
                "overlay_series_dir": str(dicom_dir.relative_to(case_dir)),
            },
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
