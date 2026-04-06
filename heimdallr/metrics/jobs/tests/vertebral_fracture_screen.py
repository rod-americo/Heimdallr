#!/usr/bin/env python3
"""Heuristic vertebral fracture screen around the thoracolumbar junction."""

from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from nibabel.orientations import aff2axcodes

from heimdallr.metrics.jobs._bone_job_common import (
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    mask_complete,
    mask_complete_along_axis,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.analysis.bone_health import extract_study_technique_context
from heimdallr.metrics.jobs.tests.vertebral_fracture import (
    isolate_vertebral_body,
    refine_classification_with_adjacent_reference,
    screen_vertebral_fracture,
    vertebra_level_index,
)
from heimdallr.metrics.jobs.tests._vertebral_fracture_overlay_text import (
    build_pathology_label,
    build_overlay_title,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import create_secondary_capture_from_rgb
from heimdallr.shared.paths import study_artifacts_dir


SERIES_NUMBER = 9107


def _vertebra_sort_key(level: str) -> tuple[int, int, str]:
    index = vertebra_level_index(level)
    if index is None:
        return (1, 10_000, str(level))
    return (0, int(index), str(level))


def discover_available_vertebrae(artifacts_dir: Path) -> list[str]:
    total_dir = artifacts_dir / "total"
    discovered: list[str] = []
    for mask_path in total_dir.glob("vertebrae_*.nii.gz"):
        prefix = "vertebrae_"
        suffix = ".nii.gz"
        name = mask_path.name
        if not name.startswith(prefix) or not name.endswith(suffix):
            continue
        vertebra = name[len(prefix):-len(suffix)].strip().upper()
        if vertebra:
            discovered.append(vertebra)
    return sorted(set(discovered), key=_vertebra_sort_key)


def infer_patient_axes_from_affine(affine: np.ndarray) -> tuple[int | None, int | None]:
    """Resolve AP and SI array axes from the image affine when available."""
    try:
        axis_codes = aff2axcodes(affine)
    except Exception:
        return None, None

    ap_axis = None
    si_axis = None
    for axis_index, axis_code in enumerate(axis_codes):
        code = str(axis_code or "").upper()
        if code in {"A", "P"}:
            ap_axis = axis_index
        elif code in {"S", "I"}:
            si_axis = axis_index
    return ap_axis, si_axis


def screen_single_vertebra(
    vertebra: str,
    mask_path_str: str,
    ct_shape: tuple[int, ...],
    spacing_mm: tuple[float, float, float],
    ap_axis: int | None,
    si_axis: int | None,
) -> dict:
    """Load one vertebral mask and run the screen in an isolated worker."""
    mask_path = Path(mask_path_str)
    if not mask_path.exists():
        return {
            "vertebra": vertebra,
            "available": False,
            "summary": {"job_status": "missing_mask"},
        }

    _, mask = load_nifti_mask(mask_path)
    if tuple(mask.shape) != tuple(ct_shape):
        return {
            "vertebra": vertebra,
            "available": False,
            "summary": {"job_status": "shape_mismatch"},
        }

    complete_mask = (
        mask_complete_along_axis(mask, si_axis)
        if si_axis is not None
        else mask_complete(mask)
    )
    if not complete_mask:
        return {
            "vertebra": vertebra,
            "available": True,
            "mask_path": str(mask_path),
            "analysis_eligible": False,
            "summary": {
                "job_name": "vertebral_fracture_screen",
                "status": "indeterminate",
                "screen_label": "indeterminate",
                "screen_confidence": 0.0,
                "genant_grade": None,
                "genant_label": "indeterminate",
                "severity": "indeterminate",
                "suspected_pattern": None,
                "body_isolation": {},
                "morphometry": {},
                "ratios": {},
                "qc_flags": ["mask_incomplete"],
                "mask_complete": False,
            },
        }

    screen = screen_vertebral_fracture(
        mask,
        spacing_mm=spacing_mm,
        ap_axis=ap_axis,
        si_axis=si_axis,
    )
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
    return {
        "vertebra": vertebra,
        "available": True,
        "mask_path": str(mask_path),
        "analysis_eligible": True,
        "summary": summarized,
    }


def render_fracture_overlay_rgb(
    ct_plane: np.ndarray,
    vertebra_overlays: list[dict],
    title: str,
    aspect: float,
) -> np.ndarray:
    rotated_ct = np.fliplr(np.rot90(np.asarray(ct_plane, dtype=np.float32)))
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
        rotated_mask = np.fliplr(np.rot90(np.asarray(overlay["mask_plane"], dtype=bool)))
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


def remaining_axis(ap_axis: int, si_axis: int) -> int:
    return next(axis for axis in range(3) if axis not in (ap_axis, si_axis))


def reorient_volume(data: np.ndarray, ap_axis: int, si_axis: int) -> np.ndarray:
    lateral_axis = remaining_axis(ap_axis, si_axis)
    return np.moveaxis(np.asarray(data), (ap_axis, si_axis, lateral_axis), (0, 1, 2))


def vertebral_centerline_points(
    masks_by_vertebra: dict[str, np.ndarray],
    *,
    si_axis: int,
    lateral_axis: int,
) -> list[tuple[float, float, str]]:
    points: list[tuple[float, float, str]] = []
    for vertebra, mask in masks_by_vertebra.items():
        coords = np.argwhere(np.asarray(mask, dtype=bool))
        if coords.size == 0:
            continue
        center = coords.mean(axis=0)
        points.append((float(center[si_axis]), float(center[lateral_axis]), vertebra))
    return sorted(points, key=lambda item: item[0])


def build_centerline_sagittal_slab(
    ct_data: np.ndarray,
    geometry_masks_by_vertebra: dict[str, np.ndarray],
    overlay_masks_by_vertebra: dict[str, np.ndarray],
    *,
    spacing_mm: tuple[float, float, float],
    ap_axis: int,
    si_axis: int,
    slab_thickness_mm: float = 5.0,
    crop_padding_voxels: int = 6,
) -> tuple[np.ndarray | None, dict[str, np.ndarray], float]:
    if not geometry_masks_by_vertebra or not overlay_masks_by_vertebra:
        return None, {}, 1.0

    lateral_axis = remaining_axis(ap_axis, si_axis)
    centerline_points = vertebral_centerline_points(
        geometry_masks_by_vertebra,
        si_axis=si_axis,
        lateral_axis=lateral_axis,
    )
    if not centerline_points:
        return None, {}, 1.0

    combined_mask = np.zeros_like(next(iter(geometry_masks_by_vertebra.values())), dtype=bool)
    for mask in geometry_masks_by_vertebra.values():
        combined_mask |= np.asarray(mask, dtype=bool)

    combined_reoriented = reorient_volume(combined_mask, ap_axis, si_axis)
    ap_positions = np.where(combined_reoriented.any(axis=(1, 2)))[0]
    si_positions = np.where(combined_reoriented.any(axis=(0, 2)))[0]
    if ap_positions.size == 0 or si_positions.size == 0:
        return None, {}, 1.0

    ap_start = max(0, int(ap_positions[0]) - int(crop_padding_voxels))
    ap_stop = min(int(combined_reoriented.shape[0]), int(ap_positions[-1]) + int(crop_padding_voxels) + 1)
    si_start = max(0, int(si_positions[0]) - int(crop_padding_voxels))
    si_stop = min(int(combined_reoriented.shape[1]), int(si_positions[-1]) + int(crop_padding_voxels) + 1)
    if ap_stop <= ap_start or si_stop <= si_start:
        return None, {}, 1.0

    ct_reoriented = reorient_volume(ct_data, ap_axis, si_axis)
    ct_crop = np.asarray(ct_reoriented[ap_start:ap_stop, si_start:si_stop, :], dtype=np.float32)
    mask_crops = {
        vertebra: reorient_volume(mask, ap_axis, si_axis)[ap_start:ap_stop, si_start:si_stop, :]
        for vertebra, mask in overlay_masks_by_vertebra.items()
    }

    centerline_si = np.asarray([point[0] for point in centerline_points], dtype=np.float64)
    centerline_lateral = np.asarray([point[1] for point in centerline_points], dtype=np.float64)
    si_global = np.arange(si_start, si_stop, dtype=np.float64)
    if centerline_si.size == 1:
        slab_centers = np.full(si_global.shape, centerline_lateral[0], dtype=np.float64)
    else:
        slab_centers = np.interp(
            si_global,
            centerline_si,
            centerline_lateral,
            left=float(centerline_lateral[0]),
            right=float(centerline_lateral[-1]),
        )

    lateral_spacing_mm = float(spacing_mm[lateral_axis]) if spacing_mm[lateral_axis] > 0 else 1.0
    slab_width_vox = max(1, int(round(float(slab_thickness_mm) / lateral_spacing_mm)))

    slab_plane = np.zeros((ct_crop.shape[0], ct_crop.shape[1]), dtype=np.float32)
    overlay_planes = {
        vertebra: np.zeros((ct_crop.shape[0], ct_crop.shape[1]), dtype=bool)
        for vertebra in mask_crops
    }
    for si_local, center_lateral in enumerate(slab_centers):
        left = int(np.floor(center_lateral - (slab_width_vox / 2.0)))
        right = left + slab_width_vox
        left = max(0, left)
        right = min(int(ct_crop.shape[2]), right)
        left = max(0, right - slab_width_vox)
        if right <= left:
            right = min(int(ct_crop.shape[2]), left + 1)

        slab_plane[:, si_local] = np.mean(ct_crop[:, si_local, left:right], axis=1)
        for vertebra, mask_crop in mask_crops.items():
            overlay_planes[vertebra][:, si_local] = np.any(mask_crop[:, si_local, left:right], axis=1)

    aspect = (
        float(spacing_mm[si_axis]) / float(spacing_mm[ap_axis])
        if spacing_mm[ap_axis] > 0 and spacing_mm[si_axis] > 0
        else 1.0
    )
    return slab_plane, overlay_planes, aspect


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
        vertebrae = discover_available_vertebrae(artifacts_dir)
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
        ap_axis, si_axis = infer_patient_axes_from_affine(ct_img.affine)
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
        overlay_mask_paths_by_vertebra: dict[str, str] = {}
        available_count = len(vertebrae)
        analyzed_vertebrae: list[str] = []
        incomplete_vertebrae: list[str] = []
        max_workers = max(1, len(vertebrae))
        if vertebrae:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(
                        screen_single_vertebra,
                        vertebra,
                        str(artifacts_dir / "total" / f"vertebrae_{vertebra}.nii.gz"),
                        tuple(int(value) for value in ct_data.shape),
                        spacing,
                        ap_axis,
                        si_axis,
                    ): vertebra
                    for vertebra in vertebrae
                }
                for future in as_completed(future_map):
                    result = future.result()
                    vertebra = str(result["vertebra"])
                    per_vertebra[vertebra] = dict(result["summary"])
                    if bool(result.get("available")):
                        overlay_mask_paths_by_vertebra[vertebra] = str(result["mask_path"])
                    if bool(result.get("analysis_eligible")):
                        analyzed_vertebrae.append(vertebra)
                    elif bool(result.get("available")):
                        incomplete_vertebrae.append(vertebra)

        refined_vertebrae = refine_classification_with_adjacent_reference(
            {vertebra: per_vertebra[vertebra] for vertebra in analyzed_vertebrae if vertebra in per_vertebra}
        )
        for vertebra, summary in refined_vertebrae.items():
            per_vertebra[vertebra] = summary
        overall_suspicion = False
        highest_severity = "indeterminate"
        highest_genant_grade: int | None = None
        suspicious_levels: list[str] = []
        severity_order = {"indeterminate": -1, "none": 0, "mild": 1, "moderate": 2, "severe": 3}
        for vertebra in analyzed_vertebrae:
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
        if overlay_mask_paths_by_vertebra and emit_dicom and ap_axis is not None and si_axis is not None:
            geometry_masks_by_vertebra = {
                vertebra: load_nifti_mask(Path(mask_path_str))[1]
                for vertebra, mask_path_str in overlay_mask_paths_by_vertebra.items()
            }
            overlay_body_masks_by_vertebra = {}
            for vertebra in analyzed_vertebrae:
                mask = geometry_masks_by_vertebra.get(vertebra)
                if mask is None:
                    continue
                body_result = isolate_vertebral_body(
                    mask,
                    spacing_mm=spacing,
                    ap_axis=ap_axis,
                    si_axis=si_axis,
                )
                body_mask = np.asarray(body_result.get("body_mask"), dtype=bool)
                if np.any(body_mask):
                    overlay_body_masks_by_vertebra[vertebra] = body_mask
            slab_thickness_mm = float(job_config.get("overlay_sagittal_slab_thickness_mm", 5.0))
            ct_plane, overlay_planes, aspect = build_centerline_sagittal_slab(
                ct_data,
                geometry_masks_by_vertebra,
                overlay_body_masks_by_vertebra,
                spacing_mm=spacing,
                ap_axis=ap_axis,
                si_axis=si_axis,
                slab_thickness_mm=slab_thickness_mm,
            )
            if ct_plane is not None:
                for vertebra in analyzed_vertebrae:
                    mask_plane = overlay_planes.get(vertebra)
                    summary = per_vertebra.get(vertebra, {})
                    if mask_plane is None or not np.any(mask_plane):
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
                "job_status": "complete" if analyzed_vertebrae else "missing_masks",
                "technique_context": technique_context,
                "vertebrae_requested": vertebrae,
                "vertebrae_available_count": available_count,
                "vertebrae_analyzed": analyzed_vertebrae,
                "vertebrae_analyzed_count": len(analyzed_vertebrae),
                "vertebrae_incomplete": sorted(incomplete_vertebrae, key=_vertebra_sort_key),
                "overall_suspicion": overall_suspicion if analyzed_vertebrae else None,
                "highest_genant_grade": highest_genant_grade if analyzed_vertebrae else None,
                "highest_severity": highest_severity if analyzed_vertebrae else "indeterminate",
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
