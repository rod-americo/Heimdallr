#!/usr/bin/env python3
"""Composite opportunistic osteoporosis screening job."""

from __future__ import annotations

import json

from heimdallr.metrics.jobs._bone_job_common import (
    load_job_config,
    metric_output_dir,
    parse_args,
    read_json,
    write_payload,
)
from heimdallr.processing.bone_health import build_opportunistic_osteoporosis_composite, classify_l1_hu
from heimdallr.shared.paths import study_dir, study_results_json


def main() -> int:
    args = parse_args(__doc__ or "Composite osteoporosis job")
    _ = load_job_config(args.job_config_json)
    metric_key = "opportunistic_osteoporosis_composite"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, _, result_path = metric_output_dir(args.case_id, metric_key)
        all_results = read_json(study_results_json(args.case_id))
        metrics = all_results.get("metrics", {}) if isinstance(all_results, dict) else {}

        hu_job = metrics.get("bone_health_l1_hu", {})
        vol_job = metrics.get("bone_health_l1_volumetric", {})
        frac_job = metrics.get("vertebral_fracture_screen", {})

        hu_measurement = hu_job.get("measurement", {}) if isinstance(hu_job, dict) else {}
        vol_measurement = vol_job.get("measurement", {}) if isinstance(vol_job, dict) else {}
        frac_measurement = frac_job.get("measurement", {}) if isinstance(frac_job, dict) else {}

        hu_mean = hu_measurement.get("l1_trabecular_hu_mean")
        vol_trabecular_hu = vol_measurement.get("bone_health_l1_volumetric_trabecular_hu_mean")
        vol_full_hu = vol_measurement.get("bone_health_l1_volumetric_full_hu_mean")

        fracture_suspicion = frac_measurement.get("overall_suspicion")
        qc_pass = None
        if isinstance(hu_measurement.get("qc"), dict):
            qc_pass = hu_measurement["qc"].get("bone_health_qc_pass")
        elif isinstance(vol_measurement.get("qc"), dict):
            qc_pass = vol_measurement["qc"].get("bone_health_qc_pass")

        preferred_hu = hu_mean if hu_mean is not None else vol_trabecular_hu
        density_label = classify_l1_hu(preferred_hu if preferred_hu is not None else vol_full_hu)
        composite = build_opportunistic_osteoporosis_composite(
            l1_trabecular_hu_mean=preferred_hu,
            l1_full_hu_mean=vol_full_hu,
            fracture_suspicion=fracture_suspicion,
            qc_pass=qc_pass,
            density_label=density_label,
        )

        if fracture_suspicion is True:
            recommendation = "review_for_vertebral_fracture_and_consider_dxa"
        elif composite["opportunistic_osteoporosis_composite"] == "high":
            recommendation = "consider_dxa_or_bone_health_follow_up"
        elif composite["opportunistic_osteoporosis_composite"] == "moderate":
            recommendation = "nonurgent_bone_health_review"
        else:
            recommendation = "no_immediate_bone_health_escalation"

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": {
                "results_json": str(study_results_json(args.case_id).relative_to(study_dir(args.case_id))),
                "bone_health_l1_hu": "metadata/resultados.json#metrics.bone_health_l1_hu",
                "bone_health_l1_volumetric": "metadata/resultados.json#metrics.bone_health_l1_volumetric",
                "vertebral_fracture_screen": "metadata/resultados.json#metrics.vertebral_fracture_screen",
            },
            "measurement": {
                "job_status": "complete",
                "preferred_trabecular_hu_mean": preferred_hu,
                "fallback_full_hu_mean": vol_full_hu,
                "fracture_suspicion": fracture_suspicion,
                "qc_pass": qc_pass,
                "density_label": density_label,
                "recommendation": recommendation,
                **composite,
            },
            "artifacts": {
                "result_json": str(result_path.relative_to(case_dir)),
            },
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

