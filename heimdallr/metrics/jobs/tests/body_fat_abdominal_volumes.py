#!/usr/bin/env python3
"""Quantify abdominal body-fat compartments across vertebral slabs T12-L5."""

from __future__ import annotations

import json

from heimdallr.metrics.jobs.tests._body_fat_job_common import (
    TARGET_LEVELS,
    build_abdominal_aggregate,
    compute_level_measurements,
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    metric_output_dir,
    parse_args,
    resolve_body_fat_inputs,
    save_volumetry_profile,
    write_payload,
)


def main() -> int:
    args = parse_args(__doc__ or "Body fat abdominal volumes job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "body_fat_abdominal_volumes"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        profile_path = metric_dir / "profile.png"
        inputs = resolve_body_fat_inputs(args.case_id)
        ct_path = inputs["ct_path"]
        sat_path = inputs["subcutaneous_fat_path"]
        torso_path = inputs["torso_fat_path"]
        level_paths = inputs["level_paths"]

        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "subcutaneous_fat_mask": str(sat_path.relative_to(case_dir)) if sat_path.exists() else None,
            "torso_fat_mask": str(torso_path.relative_to(case_dir)) if torso_path.exists() else None,
            "slab_definition": "vertebral_mask_axial_extent",
            "levels": list(TARGET_LEVELS),
        }

        if ct_path is None or not ct_path.exists() or not sat_path.exists() or not torso_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_inputs"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        _, ct_data = load_ct_volume(ct_path)
        sat_img, sat_mask = load_nifti_mask(sat_path)
        torso_img, torso_mask = load_nifti_mask(torso_path)
        if sat_mask.shape != torso_mask.shape or ct_data.shape != sat_mask.shape:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "shape_mismatch"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        level_masks = {}
        for level, level_path in level_paths.items():
            if level_path.exists():
                _, level_mask = load_nifti_mask(level_path)
                if level_mask.shape == sat_mask.shape:
                    level_masks[level] = level_mask

        level_measurements, complete_levels, measurable_levels = compute_level_measurements(
            level_masks=level_masks,
            sat_mask=sat_mask,
            torso_mask=torso_mask,
            spacing_xyz=tuple(float(value) for value in sat_img.header.get_zooms()[:3]),
        )
        aggregate = build_abdominal_aggregate(
            level_measurements=level_measurements,
            complete_levels=complete_levels,
            measurable_levels=measurable_levels,
            sat_mask=sat_mask,
            torso_mask=torso_mask,
            spacing_xyz=tuple(float(value) for value in sat_img.header.get_zooms()[:3]),
        )

        measurement = {
            "job_status": aggregate.get("job_status", "indeterminate"),
            "source_masks": {
                "subcutaneous_fat": "subcutaneous_fat",
                "torso_fat": "torso_fat",
                "visceral_proxy": "torso_fat",
            },
            "levels_requested": list(TARGET_LEVELS),
            "levels_complete": complete_levels,
            "levels_measurable": measurable_levels,
            "aggregate": aggregate,
            "levels": level_measurements,
        }

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        if job_config.get("generate_overlay", True):
            save_volumetry_profile(level_measurements, aggregate, profile_path)
            artifacts["overlay_png"] = str(profile_path.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": measurement,
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
