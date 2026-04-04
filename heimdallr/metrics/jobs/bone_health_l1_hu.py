#!/usr/bin/env python3
"""Opportunistic L1 trabecular HU screening job."""

from __future__ import annotations

import json

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    build_l1_sagittal_roi,
    extract_plane,
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    sagittal_plane_spacing_mm,
    write_payload,
)
from heimdallr.metrics.jobs._bone_health_overlay_text import (
    build_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.metrics.analysis.bone_health import (
    build_bone_health_qc_flags,
    calculate_mask_hu_statistics,
    classify_l1_hu,
    extract_study_technique_context,
)
from heimdallr.shared.paths import study_artifacts_dir


SERIES_NUMBER = 9106


def render_sagittal_overlay_rgb(
    ct_plane: np.ndarray,
    overlay_mask: np.ndarray,
    mask_outline: np.ndarray,
    title: str,
    summary_lines: list[str],
    plane_spacing_mm: tuple[float, float],
) -> np.ndarray:
    rotated_ct = np.rot90(np.asarray(ct_plane, dtype=np.float32))
    rotated_overlay = np.rot90(np.asarray(overlay_mask, dtype=bool))
    rotated_outline = np.rot90(np.asarray(mask_outline, dtype=bool))
    aspect = (
        float(plane_spacing_mm[1]) / float(plane_spacing_mm[0])
        if plane_spacing_mm[0] > 0 and plane_spacing_mm[1] > 0
        else 1.0
    )

    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(rotated_ct, cmap="gray", vmin=-250.0, vmax=1250.0, interpolation="nearest", aspect=aspect)

    if rotated_overlay.any():
        masked = np.ma.masked_where(~rotated_overlay, rotated_overlay.astype(np.uint8))
        ax.imshow(masked, cmap="cool", alpha=0.55, interpolation="nearest", aspect=aspect)
        ax.contour(rotated_overlay, levels=[0.5], colors=["#66e0ff"], linewidths=1.1)

    if rotated_outline.any():
        ax.contour(rotated_outline, levels=[0.5], colors=["#ffd166"], linewidths=0.9)

    ax.set_title(title, fontsize=13, color="white")
    ax.text(
        0.03,
        0.97,
        "\n".join(summary_lines),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=10,
        color="white",
        bbox={
            "boxstyle": "round,pad=0.4",
            "facecolor": "black",
            "alpha": 0.55,
            "edgecolor": "none",
        },
    )
    ax.axis("off")
    fig.tight_layout()
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def main() -> int:
    args = parse_args(__doc__ or "L1 HU job")
    job_config = load_job_config(args.job_config_json)
    metric_key = "bone_health_l1_hu"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}
    case_dir = None
    result_path = None

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        l1_path = artifacts_dir / "total" / "vertebrae_L1.nii.gz"
        overlay_sc_path = metric_dir / "overlay_sc.dcm"

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
        _, l1_mask = load_nifti_mask(l1_path)
        if ct_data.shape != l1_mask.shape:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "shape_mismatch"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

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

        roi_mask_2d, roi_info = build_l1_sagittal_roi(
            l1_mask,
            spacing,
            erosion_mm=float(job_config.get("erosion_mm", 5.0)),
            roi_radius_mm=float(job_config.get("roi_radius_mm", 6.0)),
        )
        completeness = mask_complete(l1_mask)

        if roi_mask_2d is None:
            measurement = {
                "job_status": roi_info.get("status", "indeterminate"),
                "plane": roi_info.get("plane", "sagittal"),
                "plane_axis": roi_info.get("plane_axis"),
                "plane_index": roi_info.get("plane_index"),
                "mask_complete": completeness,
                "technique_context": technique_context,
            }
            payload["status"] = "done"
            payload["measurement"] = measurement
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        plane_axis = str(roi_info["plane_axis"])
        plane_index = int(roi_info["plane_index"])
        total_planes = int(ct_data.shape[0] if plane_axis == "x" else ct_data.shape[1])
        plane_spacing = sagittal_plane_spacing_mm(spacing, plane_axis)
        mask_plane = np.asarray(extract_plane(l1_mask, plane_axis, plane_index), dtype=bool)
        ct_plane = np.asarray(extract_plane(ct_data, plane_axis, plane_index), dtype=np.float32)
        slice_stats = calculate_mask_hu_statistics(ct_plane, roi_mask_2d)
        qc = build_bone_health_qc_flags(
            context=technique_context,
            full_mask_voxel_count=int(np.count_nonzero(l1_mask)),
            trabecular_voxel_count=int(slice_stats["voxel_count"]),
            mask_complete=completeness,
            strict=bool(job_config.get("strict_qc", False)),
        )
        classification = classify_l1_hu(slice_stats["mean_hu"])

        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        dicom_exports: list[dict[str, str]] = []
        emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", job_config.get("generate_overlay", True)))
        if emit_dicom:
            artifact_locale = resolve_artifact_locale(job_config)
            title, summary_lines = build_overlay_text(
                hu_mean=slice_stats["mean_hu"],
                hu_std=slice_stats["std_hu"],
                roi_voxels=int(slice_stats["voxel_count"]),
                roi_radius_mm=float(roi_info["roi_radius_mm"]),
                classification=classification,
                locale=artifact_locale,
            )
            overlay_rgb = render_sagittal_overlay_rgb(
                ct_plane=ct_plane,
                overlay_mask=roi_mask_2d,
                mask_outline=mask_plane,
                title=title,
                summary_lines=summary_lines,
                plane_spacing_mm=plane_spacing,
            )
            create_secondary_capture_from_rgb(
                overlay_rgb,
                overlay_sc_path,
                case_metadata,
                series_description=series_description(artifact_locale),
                series_number=SERIES_NUMBER,
                instance_number=1,
                derivation_description=derivation_description(
                    artifact_locale,
                    hu_mean=slice_stats["mean_hu"],
                    classification=classification,
                ),
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
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "plane": "sagittal",
                "plane_axis": plane_axis,
                "plane_index": plane_index,
                "plane_index_basis": "nifti_zero_based",
                "probable_viewer_plane_index_one_based": int(total_planes - plane_index),
                "total_planes": total_planes,
                "spacing_mm": {"x": spacing[0], "y": spacing[1], "z": spacing[2]},
                "plane_spacing_mm": {"row": plane_spacing[0], "col": plane_spacing[1]},
                "mask_complete": completeness,
                "roi_method": "single_plane_eroded_inscribed_circle_on_sagittal_center",
                "roi_status": roi_info["status"],
                "roi_center_2d": roi_info["roi_center_2d"],
                "roi_radius_mm": roi_info["roi_radius_mm"],
                "roi_max_inscribed_radius_mm": roi_info["max_inscribed_radius_mm"],
                "l1_trabecular_hu_mean": slice_stats["mean_hu"],
                "l1_trabecular_hu_std": slice_stats["std_hu"],
                "l1_trabecular_voxel_count": int(slice_stats["voxel_count"]),
                "classification": classification,
                "technique_context": technique_context,
                "qc": qc,
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
