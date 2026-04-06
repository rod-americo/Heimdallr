#!/usr/bin/env python3
"""Opportunistic volumetric L1 bone-health job."""

from __future__ import annotations

import json

import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    center_slice_index,
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
from heimdallr.metrics.analysis.bone_health import (
    build_bone_health_qc_flags,
    build_l1_trabecular_roi_mask,
    classify_l1_hu,
    compute_l1_volumetric_metrics,
    extract_study_technique_context,
)
from heimdallr.shared.paths import study_artifacts_dir


def main() -> int:
    args = parse_args(__doc__ or "L1 volumetric job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "bone_health_l1_volumetric"
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

        erosion_mm = float(job_config.get("erosion_mm", 5.0))
        volumetric = compute_l1_volumetric_metrics(
            ct=ct_data,
            mask=l1_mask,
            spacing_mm=spacing,
            erosion_mm=erosion_mm,
        )
        trabecular_mask = build_l1_trabecular_roi_mask(l1_mask, spacing_mm=spacing, erosion_mm=erosion_mm)
        completeness = mask_complete(l1_mask)
        qc = build_bone_health_qc_flags(
            context=technique_context,
            full_mask_voxel_count=int(volumetric["bone_health_l1_volumetric_full_voxel_count"]),
            trabecular_voxel_count=int(volumetric["bone_health_l1_volumetric_trabecular_voxel_count"]),
            mask_complete=completeness,
            strict=bool(job_config.get("strict_qc", False)),
        )
        classification = classify_l1_hu(volumetric["bone_health_l1_volumetric_trabecular_hu_mean"])

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        center_z = center_slice_index(l1_mask)
        if job_config.get("generate_overlay", True) and center_z is not None:
            ct_slice = np.asarray(ct_data[:, :, center_z], dtype=np.float32)
            save_png_overlay(
                ct_slice=ct_slice,
                overlay_mask=np.asarray(trabecular_mask[:, :, center_z], dtype=bool),
                output_path=overlay_path,
                title=f"L1 Volumetric ROI ({classification})",
                summary_lines=[
                    f"Trab HU: {volumetric['bone_health_l1_volumetric_trabecular_hu_mean']}",
                    f"Full HU: {volumetric['bone_health_l1_volumetric_full_hu_mean']}",
                    f"Frac: {volumetric['bone_health_l1_volumetric_trabecular_fraction']}",
                    f"QC pass: {qc['bone_health_qc_pass']}",
                ],
                mask_outline=np.asarray(l1_mask[:, :, center_z], dtype=bool),
            )
            artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "mask_complete": completeness,
                "classification": classification,
                "technique_context": technique_context,
                "qc": qc,
                **volumetric,
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
