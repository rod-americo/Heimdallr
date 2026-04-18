#!/usr/bin/env python3
"""Experimental vertebral bone-health bundle."""

from __future__ import annotations

import os
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
from scipy.ndimage import distance_transform_edt

from heimdallr.metrics.analysis.bone_health import calculate_mask_hu_statistics, extract_study_technique_context
from heimdallr.metrics.jobs._bone_health_overlay_text import (
    build_overlay_text,
    resolve_artifact_locale,
)
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
    write_payload,
)
from heimdallr.metrics.jobs.bone_health_l1_hu import render_sagittal_overlay_rgb
from heimdallr.metrics.jobs.tests.vertebral_fracture_screen import discover_available_vertebrae
from heimdallr.shared.paths import study_artifacts_dir


def _save_rgb_png(rgb: np.ndarray, output_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(np.asarray(rgb, dtype=np.uint8), interpolation="nearest")
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(output_path, dpi=180, facecolor="black", bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def _measure_complete_vertebra_from_mask(
    *,
    vertebra: str,
    ct_data: np.ndarray,
    mask: np.ndarray,
) -> dict[str, Any]:
    if ct_data.shape != mask.shape:
        return {
            "vertebra": vertebra,
            "status": "shape_mismatch",
            "mask_complete": False,
            "included": False,
        }

    complete = mask_complete(mask)
    stats = calculate_mask_hu_statistics(ct_data, mask)
    return {
        "vertebra": vertebra,
        "status": "done",
        "mask_complete": complete,
        "included": bool(complete and stats["voxel_count"] > 0),
        "full_hu_mean": stats["mean_hu"],
        "full_hu_std": stats["std_hu"],
        "full_voxel_count": int(stats["voxel_count"]),
        "method": "full_mask_no_erosion",
    }


def _measure_complete_vertebra(
    *,
    vertebra: str,
    ct_data: np.ndarray,
    mask_path: Path,
) -> dict[str, Any]:
    _, mask = load_nifti_mask(mask_path)
    return _measure_complete_vertebra_from_mask(
        vertebra=vertebra,
        ct_data=ct_data,
        mask=mask,
    )


def _erode_mask_mm(
    mask: np.ndarray,
    *,
    spacing_mm: tuple[float, float, float],
    erosion_mm: float,
) -> np.ndarray:
    mask_bool = np.asarray(mask, dtype=bool)
    if erosion_mm <= 0.0 or not np.any(mask_bool):
        return mask_bool
    coords = np.argwhere(mask_bool)
    mins = coords.min(axis=0)
    maxs = coords.max(axis=0) + 1
    cropped = mask_bool[mins[0]:maxs[0], mins[1]:maxs[1], mins[2]:maxs[2]]
    distance_mm = distance_transform_edt(cropped, sampling=spacing_mm)
    eroded_cropped = np.asarray(distance_mm >= float(erosion_mm), dtype=bool)
    eroded = np.zeros_like(mask_bool, dtype=bool)
    eroded[mins[0]:maxs[0], mins[1]:maxs[1], mins[2]:maxs[2]] = eroded_cropped
    return eroded


def _build_attenuation_variants(
    *,
    vertebra: str,
    ct_data: np.ndarray,
    mask_path: Path,
    spacing_mm: tuple[float, float, float],
) -> list[dict[str, Any]]:
    _, mask = load_nifti_mask(mask_path)
    return _build_attenuation_variants_from_mask(
        vertebra=vertebra,
        ct_data=ct_data,
        mask=mask,
        spacing_mm=spacing_mm,
    )


def _build_attenuation_variants_from_mask(
    *,
    vertebra: str,
    ct_data: np.ndarray,
    mask: np.ndarray,
    spacing_mm: tuple[float, float, float],
) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for erosion_mm in (0, 1, 2, 3, 4, 5):
        if erosion_mm == 0:
            label = vertebra
            variant_mask = np.asarray(mask, dtype=bool)
        else:
            label = f"{vertebra}_{erosion_mm}"
            variant_mask = _erode_mask_mm(mask, spacing_mm=spacing_mm, erosion_mm=float(erosion_mm))
        stats = calculate_mask_hu_statistics(ct_data, variant_mask)
        variants.append(
            {
                "label": label,
                "vertebra": vertebra,
                "erosion_mm": int(erosion_mm),
                "mean_hu": stats["mean_hu"],
                "std_hu": stats["std_hu"],
                "voxel_count": int(stats["voxel_count"]),
                "method": "3d_volume_attenuation_mean",
            }
        )
    return variants


def _analyze_vertebra(
    *,
    vertebra: str,
    ct_data: np.ndarray,
    mask_path: Path,
    spacing_mm: tuple[float, float, float],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _, mask = load_nifti_mask(mask_path)
    measurement = _measure_complete_vertebra_from_mask(
        vertebra=vertebra,
        ct_data=ct_data,
        mask=mask,
    )
    if not measurement["included"]:
        return measurement, []
    variants = _build_attenuation_variants_from_mask(
        vertebra=vertebra,
        ct_data=ct_data,
        mask=mask,
        spacing_mm=spacing_mm,
    )
    return measurement, variants


def _build_l1_roi_overlay(
    *,
    case_dir: Path,
    ct_data: np.ndarray,
    ct_affine: np.ndarray,
    spacing: tuple[float, float, float],
    l1_mask_path: Path,
    metric_dir: Path,
    artifact_locale: str,
    roi_erosion_mm: float,
    roi_radius_mm: float,
) -> tuple[dict[str, Any], dict[str, str]]:
    _, l1_mask = load_nifti_mask(l1_mask_path)
    roi_mask_2d, roi_info = build_l1_sagittal_roi(
        l1_mask,
        spacing,
        affine=ct_affine,
        erosion_mm=roi_erosion_mm,
        roi_radius_mm=roi_radius_mm,
    )
    measurement: dict[str, Any] = {
        "status": "missing_l1_roi",
        "mask_complete": mask_complete(l1_mask),
    }
    artifacts: dict[str, str] = {}
    if roi_mask_2d is None:
        measurement.update(
            {
                "status": roi_info.get("status", "indeterminate"),
                "plane_axis": roi_info.get("plane_axis"),
                "plane_index": roi_info.get("plane_index"),
            }
        )
        return measurement, artifacts

    plane_axis = str(roi_info["plane_axis"])
    plane_index = int(roi_info["plane_index"])
    plane_spacing = (
        (spacing[1], spacing[2]) if plane_axis == "x" else (spacing[0], spacing[2])
    )
    source_axis_codes = (
        ("P", "S") if plane_axis == "x" else ("L", "S")
    )
    ct_plane = np.asarray(extract_plane(ct_data, plane_axis, plane_index), dtype=np.float32)
    mask_plane = np.asarray(extract_plane(l1_mask, plane_axis, plane_index), dtype=bool)
    roi_stats = calculate_mask_hu_statistics(ct_plane, roi_mask_2d)
    title, summary_lines = build_overlay_text(
        hu_mean=roi_stats["mean_hu"],
        hu_std=roi_stats["std_hu"],
        locale=artifact_locale,
    )
    overlay_rgb = render_sagittal_overlay_rgb(
        ct_plane=ct_plane,
        overlay_mask=roi_mask_2d,
        mask_outline=mask_plane,
        title=title,
        summary_lines=summary_lines,
        plane_spacing_mm=plane_spacing,
        source_axis_codes=source_axis_codes,
    )
    overlay_path = metric_dir / "l1_overlay.png"
    _save_rgb_png(overlay_rgb, overlay_path)
    artifacts["l1_overlay_png"] = str(overlay_path.relative_to(case_dir))
    measurement.update(
        {
            "status": "done",
            "plane_axis": plane_axis,
            "plane_index": plane_index,
            "roi_status": roi_info["status"],
            "roi_center_2d": roi_info.get("roi_center_2d"),
            "roi_radius_mm": roi_info.get("roi_radius_mm"),
            "roi_max_inscribed_radius_mm": roi_info.get("max_inscribed_radius_mm"),
            "l1_trabecular_hu_mean": roi_stats["mean_hu"],
            "l1_trabecular_hu_std": roi_stats["std_hu"],
            "l1_trabecular_voxel_count": int(roi_stats["voxel_count"]),
            "method": "validated_l1_roi",
        }
    )
    return measurement, artifacts


def main() -> int:
    args = parse_args(__doc__ or "Experimental vertebral bone-health bundle")
    job_config = load_job_config(args.job_config_json)
    metric_key = "vertebral_bone_health_bundle"
    payload = {"metric_key": metric_key, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, metric_key)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        vertebrae = discover_available_vertebrae(artifacts_dir)

        payload["inputs"] = {
            "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path and ct_path.exists() else None,
            "vertebrae": [f"artifacts/total/vertebrae_{level}.nii.gz" for level in vertebrae],
        }

        if ct_path is None or not ct_path.exists() or not vertebrae:
            payload["status"] = "skipped"
            payload["measurement"] = {"job_status": "missing_inputs"}
            payload["artifacts"] = {"result_json": str(result_path.relative_to(case_dir))}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        spacing = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        bundle = load_case_json_bundle(args.case_id)
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

        per_vertebra: list[dict[str, Any]] = []
        attenuation_variants: list[dict[str, Any]] = []
        requested_workers = int(job_config.get("workers", 12))
        max_workers = max(1, min(requested_workers, len(vertebrae), os.cpu_count() or requested_workers))
        if max_workers == 1:
            for vertebra in vertebrae:
                mask_path = artifacts_dir / "total" / f"vertebrae_{vertebra}.nii.gz"
                measurement, variants = _analyze_vertebra(
                    vertebra=vertebra,
                    ct_data=ct_data,
                    mask_path=mask_path,
                    spacing_mm=spacing,
                )
                per_vertebra.append(measurement)
                attenuation_variants.extend(variants)
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        _analyze_vertebra,
                        vertebra=vertebra,
                        ct_data=ct_data,
                        mask_path=artifacts_dir / "total" / f"vertebrae_{vertebra}.nii.gz",
                        spacing_mm=spacing,
                    )
                    for vertebra in vertebrae
                ]
                for future in futures:
                    measurement, variants = future.result()
                    per_vertebra.append(measurement)
                    attenuation_variants.extend(variants)

        artifact_locale = resolve_artifact_locale(job_config)
        l1_measurement = {"status": "missing_l1"}
        artifacts = {"result_json": str(result_path.relative_to(case_dir))}
        l1_mask_path = artifacts_dir / "total" / "vertebrae_L1.nii.gz"
        if l1_mask_path.exists():
            l1_measurement, l1_artifacts = _build_l1_roi_overlay(
                case_dir=case_dir,
                ct_data=ct_data,
                ct_affine=ct_img.affine,
                spacing=spacing,
                l1_mask_path=l1_mask_path,
                metric_dir=metric_dir,
                artifact_locale=artifact_locale,
                roi_erosion_mm=float(job_config.get("roi_erosion_mm", 5.0)),
                roi_radius_mm=float(job_config.get("roi_radius_mm", 6.0)),
            )
            artifacts.update(l1_artifacts)

        payload = {
            "metric_key": metric_key,
            "status": "done",
            "case_id": args.case_id,
            "inputs": payload["inputs"],
            "measurement": {
                "job_status": "complete",
                "technique_context": technique_context,
                "vertebrae_available_count": len(vertebrae),
                "vertebrae_complete_count": sum(1 for item in per_vertebra if item["included"]),
                "workers_used": max_workers,
                "vertebral_mean_method": "full_mask_no_erosion_complete_masks_only",
                "attenuation_variants_method": "3d_volume_attenuation_mean_with_mm_erosions",
                "l1_roi_method": "validated_existing_l1_roi",
                "l1_roi": l1_measurement,
                "per_vertebra": per_vertebra,
                "attenuation_variants": attenuation_variants,
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
