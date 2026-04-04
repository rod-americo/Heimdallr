#!/usr/bin/env python3
"""Heuristic vertebral fracture screen around the thoracolumbar junction."""

from __future__ import annotations

import json

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    extract_plane,
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    sagittal_plane_from_mask,
    sagittal_plane_spacing_mm,
    write_payload,
)
from heimdallr.metrics.analysis.bone_health import extract_study_technique_context
from heimdallr.metrics.analysis.vertebral_fracture import screen_vertebral_fracture
from heimdallr.metrics.jobs._vertebral_fracture_overlay_text import (
    build_overlay_title,
    build_panel_lines,
    build_panel_title,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
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


SERIES_NUMBER = 9107


def render_fracture_panel_rgb(vertebra_panels: list[dict], title: str) -> np.ndarray:
    fig, axes = plt.subplots(1, len(vertebra_panels), figsize=(5 * len(vertebra_panels), 6), facecolor="black")
    if len(vertebra_panels) == 1:
        axes = [axes]

    fig.patch.set_facecolor("black")
    for ax, panel in zip(axes, vertebra_panels):
        plane = np.rot90(np.asarray(panel["ct_plane"], dtype=np.float32))
        mask_plane = np.rot90(np.asarray(panel["mask_plane"], dtype=bool))
        ax.set_facecolor("black")
        ax.imshow(
            plane,
            cmap="gray",
            vmin=-250,
            vmax=1250,
            interpolation="nearest",
            aspect=float(panel.get("aspect", 1.0)),
        )
        if mask_plane.any():
            ax.contour(mask_plane, levels=[0.5], colors=["#ff7b7b"], linewidths=1.2)
        ax.set_title(panel["title"], fontsize=12, color="white")
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

    fig.suptitle(title, fontsize=15, color="white")
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.95))
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def main() -> int:
    args = parse_args(__doc__ or "Fracture screen job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "vertebral_fracture_screen"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}
    case_dir = None
    result_path = None

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        vertebrae = list(job_config.get("vertebrae", ["T12", "L1", "L2"]))
        overlay_sc_path = metric_dir / "overlay_sc.dcm"

        if ct_path is None or not ct_path.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_ct"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        bundle = load_case_json_bundle(args.case_id)
        case_metadata = {}
        case_metadata.update(bundle["id_json"])
        case_metadata.update(bundle["metadata_json"])
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
        artifact_locale = resolve_artifact_locale(job_config)

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

            mask_plane, plane_index, plane_axis = sagittal_plane_from_mask(mask)
            if mask_plane is None or plane_index is None or plane_axis is None:
                continue
            ct_plane = np.asarray(extract_plane(ct_data, plane_axis, plane_index), dtype=np.float32)
            plane_spacing = sagittal_plane_spacing_mm(spacing, plane_axis)
            aspect = (
                float(plane_spacing[1]) / float(plane_spacing[0])
                if plane_spacing[0] > 0 and plane_spacing[1] > 0
                else 1.0
            )

            panels.append(
                {
                    "ct_plane": ct_plane,
                    "mask_plane": mask_plane,
                    "title": build_panel_title(vertebra, locale=artifact_locale),
                    "summary_lines": build_panel_lines(summarized, locale=artifact_locale),
                    "aspect": aspect,
                }
            )

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        dicom_exports: list[dict[str, str]] = []
        emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", job_config.get("generate_overlay", True)))
        if panels and emit_dicom:
            overlay_rgb = render_fracture_panel_rgb(
                panels,
                title=build_overlay_title(locale=artifact_locale),
            )
            create_secondary_capture_from_rgb(
                overlay_rgb,
                overlay_sc_path,
                case_metadata,
                series_description=series_description(artifact_locale),
                series_number=SERIES_NUMBER,
                instance_number=1,
                derivation_description=derivation_description(artifact_locale, vertebrae=vertebrae),
            )
            artifacts["overlay_sc_dcm"] = str(overlay_sc_path.relative_to(case_dir))
            dicom_exports.append(
                {
                    "path": artifacts["overlay_sc_dcm"],
                    "kind": "secondary_capture",
                }
            )

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
            "dicom_exports": dicom_exports,
        }
        write_payload(result_path, payload)
    except Exception as exc:
        payload["status"] = "error"
        payload.setdefault("measurement", {"job_status": "error"})
        if case_dir is not None and result_path is not None:
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
        payload["error"] = str(exc)
        if result_path is not None:
            write_payload(result_path, payload)
        print(json.dumps(payload, indent=2))
        return 1

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
