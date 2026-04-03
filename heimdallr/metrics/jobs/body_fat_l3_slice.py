#!/usr/bin/env python3
"""Measure body-fat compartments on the center slice of L3."""

from __future__ import annotations

import json

from heimdallr.metrics.jobs._body_fat_job_common import (
    center_slice_index,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    metric_output_dir,
    parse_args,
    resolve_body_fat_inputs,
    save_l3_overlay,
    slice_area_cm2,
    write_payload,
)


def main() -> int:
    args = parse_args(__doc__ or "Body fat L3 slice job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "body_fat_l3_slice"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        overlay_path = metric_dir / "overlay.png"
        inputs = resolve_body_fat_inputs(args.case_id)
        ct_path = inputs["ct_path"]
        sat_path = inputs["subcutaneous_fat_path"]
        torso_path = inputs["torso_fat_path"]
        l3_path = inputs["l3_path"]

        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "vertebra_l3_mask": str(l3_path.relative_to(case_dir)) if l3_path.exists() else None,
            "subcutaneous_fat_mask": str(sat_path.relative_to(case_dir)) if sat_path.exists() else None,
            "torso_fat_mask": str(torso_path.relative_to(case_dir)) if torso_path.exists() else None,
        }

        if ct_path is None or not ct_path.exists() or not sat_path.exists() or not torso_path.exists() or not l3_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_inputs"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        _, ct_data = load_ct_volume(ct_path)
        sat_img, sat_mask = load_nifti_mask(sat_path)
        _, torso_mask = load_nifti_mask(torso_path)
        _, l3_mask = load_nifti_mask(l3_path)
        if not (ct_data.shape == sat_mask.shape == torso_mask.shape == l3_mask.shape):
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "shape_mismatch"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        slice_index = center_slice_index(l3_mask)
        if slice_index is None:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "empty_l3_mask"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        sat_slice = sat_mask[:, :, slice_index]
        torso_slice = torso_mask[:, :, slice_index]
        l3_slice = l3_mask[:, :, slice_index]
        spacing_xy = tuple(float(value) for value in sat_img.header.get_zooms()[:2])
        total_slices = int(ct_data.shape[2])

        sat_area = slice_area_cm2(sat_slice, spacing_xy)
        torso_area = slice_area_cm2(torso_slice, spacing_xy)
        ratio = round(torso_area / sat_area, 4) if sat_area > 0 else None

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        if job_config.get("generate_overlay", True):
            save_l3_overlay(
                ct_slice=ct_data[:, :, slice_index],
                sat_slice=sat_slice,
                torso_slice=torso_slice,
                l3_slice=l3_slice,
                output_path=overlay_path,
                summary_lines=[
                    f"Slice: {slice_index}",
                    f"Probable viewer slice: {total_slices - slice_index}",
                    f"Subcutaneous area: {sat_area:.1f} cm2",
                    f"Torso area: {torso_area:.1f} cm2",
                    f"Ratio: {ratio if ratio is not None else '-'}",
                ],
            )
            artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "slice_index": int(slice_index),
                "slice_index_basis": "nifti_zero_based",
                "probable_viewer_slice_index_one_based": int(total_slices - slice_index),
                "total_slices": total_slices,
                "pixel_spacing_mm": {"x": spacing_xy[0], "y": spacing_xy[1]},
                "subcutaneous_fat_area_cm2": sat_area,
                "torso_fat_area_cm2": torso_area,
                "visceral_proxy_area_cm2": torso_area,
                "torso_to_subcutaneous_area_ratio": ratio,
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
