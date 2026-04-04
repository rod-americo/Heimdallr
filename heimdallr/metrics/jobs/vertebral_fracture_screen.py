#!/usr/bin/env python3
"""Heuristic vertebral fracture screen around the thoracolumbar junction."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.segmentation.bone_health import extract_study_technique_context
from heimdallr.segmentation.vertebral_fracture import screen_vertebral_fracture
from heimdallr.shared.paths import study_artifacts_dir


def severity_from_label(screen_label: str | None) -> str:
    label = str(screen_label or "").strip().lower()
    if label == "suspected_crush":
        return "severe"
    if label == "suspected_biconcave":
        return "moderate"
    if label == "suspected_wedge":
        return "mild"
    if label == "indeterminate":
        return "indeterminate"
    return "none"


def sagittal_slice_from_mask(mask: np.ndarray) -> tuple[np.ndarray, int]:
    mask_bool = np.asarray(mask, dtype=bool)
    coords = np.argwhere(mask_bool)
    x_min, y_min, z_min = coords.min(axis=0)
    x_max, y_max, z_max = coords.max(axis=0)
    x_span = int(x_max - x_min + 1)
    y_span = int(y_max - y_min + 1)
    if x_span <= y_span:
        center_index = int(round((x_min + x_max) / 2.0))
        return np.asarray(mask_bool[center_index, :, :], dtype=bool), center_index
    center_index = int(round((y_min + y_max) / 2.0))
    return np.asarray(mask_bool[:, center_index, :], dtype=bool), center_index


def save_fracture_panel(ct_data: np.ndarray, vertebra_panels: list[dict], output_path: Path) -> None:
    fig, axes = plt.subplots(1, len(vertebra_panels), figsize=(5 * len(vertebra_panels), 6))
    if len(vertebra_panels) == 1:
        axes = [axes]

    for ax, panel in zip(axes, vertebra_panels):
        plane = np.rot90(np.asarray(panel["ct_plane"], dtype=np.float32))
        mask_plane = np.rot90(np.asarray(panel["mask_plane"], dtype=bool))
        ax.imshow(plane, cmap="gray", vmin=-250, vmax=1250, interpolation="nearest")
        if mask_plane.any():
            ax.contour(mask_plane, levels=[0.5], colors=["#ff7b7b"], linewidths=1.2)
        ax.set_title(panel["title"], fontsize=12)
        ax.text(
            0.03,
            0.97,
            "\n".join(panel["summary_lines"]),
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
            color="white",
            bbox={
                "boxstyle": "round,pad=0.35",
                "facecolor": "black",
                "alpha": 0.55,
                "edgecolor": "none",
            },
        )
        ax.axis("off")

    fig.tight_layout()
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def main() -> int:
    args = parse_args(__doc__ or "Fracture screen job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "vertebral_fracture_screen"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        vertebrae = list(job_config.get("vertebrae", ["T12", "L1", "L2"]))
        overlay_path = metric_dir / "overlay.png"

        if ct_path is None or not ct_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_ct"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        bundle = load_case_json_bundle(args.case_id)
        spacing = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
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

        per_vertebra: dict[str, dict] = {}
        panels: list[dict] = []
        overall_suspicion = False
        highest_severity = "none"
        available_count = 0
        suspicious_levels: list[str] = []
        severity_order = {"indeterminate": -1, "none": 0, "mild": 1, "moderate": 2, "severe": 3}

        for vertebra in vertebrae:
            mask_path = artifacts_dir / "total" / f"vertebrae_{vertebra}.nii.gz"
            if not mask_path.exists():
                per_vertebra[vertebra] = {"job_status": "missing_mask"}
                continue

            _, mask = load_nifti_mask(mask_path)
            if ct_data.shape != mask.shape:
                per_vertebra[vertebra] = {"job_status": "shape_mismatch"}
                continue

            available_count += 1
            screen = screen_vertebral_fracture(mask, spacing_mm=spacing)
            summarized = {
                "job_name": screen.get("job_name"),
                "status": screen.get("status"),
                "screen_label": screen.get("screen_label"),
                "screen_confidence": screen.get("screen_confidence"),
                "suspected_pattern": screen.get("suspected_pattern"),
                "body_isolation": {
                    "original_voxels": screen.get("body_isolation", {}).get("original_voxels"),
                    "body_voxels": screen.get("body_isolation", {}).get("body_voxels"),
                    "body_fraction": screen.get("body_isolation", {}).get("body_fraction"),
                    "orientation_confidence": screen.get("body_isolation", {}).get("axis_info", {}).get("orientation_confidence"),
                },
                "morphometry": {
                    "ap_depth_mm": screen.get("morphometry", {}).get("ap_depth_mm"),
                    "anterior_height_mm": screen.get("morphometry", {}).get("anterior_height_mm"),
                    "middle_height_mm": screen.get("morphometry", {}).get("middle_height_mm"),
                    "posterior_height_mm": screen.get("morphometry", {}).get("posterior_height_mm"),
                    "orientation_confidence": screen.get("morphometry", {}).get("orientation_confidence"),
                },
                "ratios": screen.get("ratios"),
                "qc_flags": screen.get("qc_flags"),
                "mask_complete": mask_complete(mask),
            }
            summarized["severity"] = severity_from_label(summarized.get("screen_label"))
            per_vertebra[vertebra] = summarized

            if str(summarized.get("status")) == "suspected":
                overall_suspicion = True
                suspicious_levels.append(vertebra)
            if severity_order.get(summarized["severity"], -1) > severity_order.get(highest_severity, -1):
                highest_severity = summarized["severity"]

            mask_plane, plane_index = sagittal_slice_from_mask(mask)
            if mask_plane.shape[0] == ct_data.shape[1]:
                ct_plane = ct_data[plane_index, :, :]
            else:
                ct_plane = ct_data[:, plane_index, :]

            panels.append(
                {
                    "ct_plane": ct_plane,
                    "mask_plane": mask_plane,
                    "title": vertebra,
                    "summary_lines": [
                        f"Status: {summarized['status']}",
                        f"Label: {summarized['screen_label']}",
                        f"Pattern: {summarized['suspected_pattern']}",
                        f"Severity: {summarized['severity']}",
                    ],
                }
            )

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        if panels and job_config.get("generate_overlay", True):
            save_fracture_panel(ct_data, panels, overlay_path)
            artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": {
                "canonical_nifti": str(ct_path.relative_to(case_dir)),
                "vertebrae": {v: f"artifacts/total/vertebrae_{v}.nii.gz" for v in vertebrae},
            },
            "measurement": {
                "job_status": "complete" if available_count > 0 else "missing_masks",
                "technique_context": technique_context,
                "vertebrae_requested": vertebrae,
                "vertebrae_available_count": available_count,
                "overall_suspicion": overall_suspicion if available_count > 0 else None,
                "highest_severity": highest_severity if available_count > 0 else "indeterminate",
                "suspicious_levels": suspicious_levels,
                "per_vertebra": per_vertebra,
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
