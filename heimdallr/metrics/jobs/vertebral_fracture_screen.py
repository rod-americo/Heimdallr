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
from heimdallr.metrics.analysis.vertebral_fracture import (
    refine_classification_with_adjacent_reference,
    screen_vertebral_fracture,
)
from heimdallr.metrics.jobs._vertebral_fracture_overlay_text import (
    build_pathology_label,
    build_overlay_title,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.shared.paths import study_artifacts_dir


SERIES_NUMBER = 9107


def render_fracture_overlay_rgb(
    ct_plane: np.ndarray,
    vertebra_overlays: list[dict],
    title: str,
    aspect: float,
) -> np.ndarray:
    rotated_ct = np.rot90(np.asarray(ct_plane, dtype=np.float32))
    fig, ax = plt.subplots(figsize=(7, 9), facecolor="black")
    fig.patch.set_facecolor("black")
    ax.set_facecolor("black")
    ax.imshow(
        rotated_ct,
        cmap="gray",
        vmin=-250,
        vmax=1250,
        interpolation="nearest",
        aspect=aspect,
    )

    for overlay in vertebra_overlays:
        rotated_mask = np.rot90(np.asarray(overlay["mask_plane"], dtype=bool))
        if not rotated_mask.any():
            continue
        color = "#ff7b7b" if overlay.get("is_pathologic") else "#9aa0a6"
        linewidth = 1.3 if overlay.get("is_pathologic") else 0.8
        alpha = 0.95 if overlay.get("is_pathologic") else 0.55
        ax.contour(rotated_mask, levels=[0.5], colors=[color], linewidths=linewidth, alpha=alpha)

        if not overlay.get("is_pathologic"):
            continue

        coords = np.argwhere(rotated_mask)
        if coords.size == 0:
            continue
        center_y = float(np.median(coords[:, 0]))
        anchor_x = float(np.max(coords[:, 1]))
        preferred_x = anchor_x + 10.0
        ha = "left"
        if preferred_x > (rotated_mask.shape[1] - 6):
            preferred_x = float(np.min(coords[:, 1])) - 10.0
            ha = "right"

        ax.annotate(
            str(overlay["label"]),
            xy=(anchor_x, center_y),
            xytext=(preferred_x, center_y),
            textcoords="data",
            ha=ha,
            va="center",
            fontsize=9,
            color="white",
            bbox={
                "boxstyle": "round,pad=0.25",
                "facecolor": "black",
                "alpha": 0.7,
                "edgecolor": color,
            },
            arrowprops={
                "arrowstyle": "-",
                "color": color,
                "lw": 1.0,
                "shrinkA": 0,
                "shrinkB": 0,
            },
        )

    ax.set_title(title, fontsize=14, color="white")
    ax.axis("off")
    fig.tight_layout()
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
        mask_overlays: list[dict] = []
        overlay_masks_by_vertebra: dict[str, np.ndarray] = {}
        available_count = 0

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
                "genant_grade": screen.get("genant_grade"),
                "genant_label": screen.get("genant_label"),
                "severity": screen.get("severity"),
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
                    "anterior_area_voxels": screen.get("morphometry", {}).get("anterior_area_voxels"),
                    "middle_area_voxels": screen.get("morphometry", {}).get("middle_area_voxels"),
                    "posterior_area_voxels": screen.get("morphometry", {}).get("posterior_area_voxels"),
                    "orientation_confidence": screen.get("morphometry", {}).get("orientation_confidence"),
                },
                "ratios": screen.get("ratios"),
                "qc_flags": screen.get("qc_flags"),
                "mask_complete": mask_complete(mask),
            }
            per_vertebra[vertebra] = summarized
            overlay_masks_by_vertebra[vertebra] = np.asarray(mask, dtype=bool)

        per_vertebra = refine_classification_with_adjacent_reference(per_vertebra)
        overall_suspicion = False
        highest_severity = "indeterminate"
        highest_genant_grade: int | None = None
        suspicious_levels: list[str] = []
        severity_order = {"indeterminate": -1, "none": 0, "mild": 1, "moderate": 2, "severe": 3}
        for vertebra in vertebrae:
            summarized = per_vertebra.get(vertebra, {})
            if not isinstance(summarized, dict):
                continue
            if str(summarized.get("status")) == "suspected":
                overall_suspicion = True
                suspicious_levels.append(vertebra)
            if severity_order.get(str(summarized.get("severity")), -1) > severity_order.get(highest_severity, -1):
                highest_severity = str(summarized.get("severity"))
            if summarized.get("genant_grade") is not None:
                current_grade = int(summarized["genant_grade"])
                highest_genant_grade = current_grade if highest_genant_grade is None else max(highest_genant_grade, current_grade)

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        dicom_exports: list[dict[str, str]] = []
        emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", job_config.get("generate_overlay", True)))
        combined_mask = None
        if overlay_masks_by_vertebra:
            combined_mask = np.zeros_like(next(iter(overlay_masks_by_vertebra.values())), dtype=bool)
            for mask in overlay_masks_by_vertebra.values():
                combined_mask |= np.asarray(mask, dtype=bool)

        if combined_mask is not None and emit_dicom:
            plane_union, plane_index, plane_axis = sagittal_plane_from_mask(combined_mask)
            if plane_union is not None and plane_index is not None and plane_axis is not None:
                ct_plane = np.asarray(extract_plane(ct_data, plane_axis, plane_index), dtype=np.float32)
                plane_spacing = sagittal_plane_spacing_mm(spacing, plane_axis)
                aspect = (
                    float(plane_spacing[1]) / float(plane_spacing[0])
                    if plane_spacing[0] > 0 and plane_spacing[1] > 0
                    else 1.0
                )

                for vertebra in vertebrae:
                    full_mask = overlay_masks_by_vertebra.get(vertebra)
                    summary = per_vertebra.get(vertebra, {})
                    if full_mask is None:
                        continue
                    mask_plane = np.asarray(extract_plane(full_mask, plane_axis, plane_index), dtype=bool)
                    if not mask_plane.any():
                        continue
                    is_pathologic = bool((summary.get("genant_grade") or 0) >= 1)
                    mask_overlays.append(
                        {
                            "vertebra": vertebra,
                            "mask_plane": mask_plane,
                            "is_pathologic": is_pathologic,
                            "label": build_pathology_label(vertebra, summary, locale=artifact_locale),
                        }
                    )

                overlay_rgb = render_fracture_overlay_rgb(
                    ct_plane,
                    mask_overlays,
                    aspect=aspect,
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
                "highest_genant_grade": highest_genant_grade if available_count > 0 else None,
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
