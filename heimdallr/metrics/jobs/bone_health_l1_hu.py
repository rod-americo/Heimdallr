#!/usr/bin/env python3
"""Opportunistic L1 trabecular HU screening job."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    build_l1_axial_roi,
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    save_png_overlay,
    write_payload,
)
from heimdallr.processing.bone_health import (
    build_bone_health_qc_flags,
    calculate_mask_hu_statistics,
    classify_l1_hu,
    extract_study_technique_context,
)
from heimdallr.shared.paths import study_artifacts_dir


def main() -> int:
    args = parse_args(__doc__ or "L1 HU job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "bone_health_l1_hu"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        l1_path = artifacts_dir / "total" / "vertebrae_L1.nii.gz"
        overlay_path = metric_dir / "overlay.png"

        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "vertebra_l1_mask": str(l1_path.relative_to(case_dir)) if l1_path.exists() else None,
        }

        if ct_path is None or not ct_path.exists() or not l1_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_inputs"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        l1_img, l1_mask = load_nifti_mask(l1_path)
        if ct_data.shape != l1_mask.shape:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "shape_mismatch"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        bundle = load_case_json_bundle(args.case_id)
        spacing = tuple(float(value) for value in l1_img.header.get_zooms()[:3])
        technique_context = extract_study_technique_context(
            id_data=bundle["id_json"],
            results={
                "slice_thickness_mm": spacing[2],
                "spacing_mm": spacing,
                "SelectedPhase": (
                    bundle["id_json"].get("Pipeline", {})
                    .get("series_selection", {})
                    .get("SelectedPhase")
                ),
            },
        )

        roi_mask_2d, roi_info = build_l1_axial_roi(l1_mask, spacing)
        completeness = mask_complete(l1_mask)

        if roi_mask_2d is None:
            measurement = {
                "job_status": roi_info.get("status", "indeterminate"),
                "mask_complete": completeness,
                "technique_context": technique_context,
            }
            payload["status"] = "done"
            payload["measurement"] = measurement
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        slice_index = int(roi_info["slice_index"])
        slice_mask = np.asarray(l1_mask[:, :, slice_index], dtype=bool)
        ct_slice = np.asarray(ct_data[:, :, slice_index], dtype=np.float32)
        slice_stats = calculate_mask_hu_statistics(ct_slice, roi_mask_2d)
        qc = build_bone_health_qc_flags(
            context=technique_context,
            full_mask_voxel_count=int(np.count_nonzero(l1_mask)),
            trabecular_voxel_count=int(slice_stats["voxel_count"]),
            mask_complete=completeness,
            strict=bool(job_config.get("strict_qc", False)),
        )
        classification = classify_l1_hu(slice_stats["mean_hu"])

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        if job_config.get("generate_overlay", True):
            title = f"L1 HU ROI ({classification})"
            summary_lines = [
                f"HU mean: {slice_stats['mean_hu']}",
                f"HU std: {slice_stats['std_hu']}",
                f"ROI voxels: {slice_stats['voxel_count']}",
                f"Slice: {slice_index}",
                f"QC pass: {qc['bone_health_qc_pass']}",
            ]
            save_png_overlay(
                ct_slice=ct_slice,
                overlay_mask=roi_mask_2d,
                output_path=overlay_path,
                title=title,
                summary_lines=summary_lines,
                mask_outline=slice_mask,
            )
            artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "slice_index": slice_index,
                "slice_index_basis": "nifti_zero_based",
                "mask_complete": completeness,
                "roi_status": roi_info["status"],
                "roi_center_xy": roi_info["roi_center_xy"],
                "roi_radius_xy": roi_info["roi_radius_xy"],
                "l1_trabecular_hu_mean": slice_stats["mean_hu"],
                "l1_trabecular_hu_std": slice_stats["std_hu"],
                "l1_trabecular_voxel_count": int(slice_stats["voxel_count"]),
                "classification": classification,
                "technique_context": technique_context,
                "qc": qc,
            },
            "artifacts": artifacts,
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

