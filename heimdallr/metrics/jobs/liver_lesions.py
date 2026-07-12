#!/usr/bin/env python3
"""Hepatic lesion summary and spatial overlays from TotalSegmentator liver_lesions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from PIL import Image
from pydicom.uid import generate_uid
from scipy.ndimage import label as ndlabel

from heimdallr.metrics.jobs._bone_job_common import (
    display_aspect_from_spacing_mm,
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    metric_output_dir,
    parse_args,
    plane_source_axis_codes,
    reorient_display_array,
    reorient_display_spacing_mm,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import (
    axial_dicom_geometry_from_nifti,
    create_secondary_capture_from_rgb,
    load_source_dicom_geometry,
    nearest_source_dicom_geometry,
    nifti_voxel_position_lps,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._liver_lesions_overlay_text import (
    build_component_overlay_text,
    derivation_description,
    overlay_title,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_artifacts_dir


METRIC_KEY = "liver_lesions"
SERIES_NUMBER = 9132
SOFT_TISSUE_WINDOW_LEVEL_HU = 60.0
SOFT_TISSUE_WINDOW_WIDTH_HU = 400.0
SOFT_TISSUE_WINDOW_LIMITS_HU = (-140.0, 260.0)


def _relpath(case_dir: Path, path: Path) -> str:
    try:
        return str(path.relative_to(case_dir))
    except ValueError:
        return str(path)


def _clear_metric_dir(metric_dir: Path) -> None:
    for path in metric_dir.iterdir():
        if path.is_file():
            path.unlink()


def _load_mask(path: Path, reference_shape: tuple[int, ...]) -> tuple[np.ndarray, dict[str, Any]]:
    status: dict[str, Any] = {"path": str(path)}
    if not path.exists():
        status.update({"status": "missing", "voxel_count": 0})
        return np.zeros(reference_shape, dtype=bool), status
    try:
        _image, mask = load_nifti_mask(path)
    except Exception as exc:
        status.update({"status": "read_error", "error": str(exc), "voxel_count": 0})
        return np.zeros(reference_shape, dtype=bool), status
    if tuple(mask.shape[:3]) != reference_shape:
        status.update(
            {
                "status": "geometry_mismatch",
                "shape": [int(value) for value in mask.shape[:3]],
                "reference_shape": [int(value) for value in reference_shape],
                "voxel_count": 0,
            }
        )
        return np.zeros(reference_shape, dtype=bool), status
    mask = np.asarray(mask, dtype=bool)
    voxel_count = int(np.count_nonzero(mask))
    status.update({"status": "present" if voxel_count else "empty", "voxel_count": voxel_count})
    return mask, status


def _label_components(mask: np.ndarray, voxel_volume_cm3: float) -> tuple[np.ndarray, list[dict[str, Any]]]:
    labeled, count = ndlabel(
        np.asarray(mask, dtype=bool),
        structure=np.ones((3, 3, 3), dtype=np.uint8),
    )
    components: list[dict[str, Any]] = []
    for component_id in range(1, int(count) + 1):
        coords = np.argwhere(labeled == component_id)
        if not coords.size:
            continue
        voxel_count = int(coords.shape[0])
        z_values = coords[:, 2]
        components.append(
            {
                "component_id": component_id,
                "voxel_count": voxel_count,
                "volume_cm3": float(voxel_count * voxel_volume_cm3),
                "centroid_index_xyz": [float(value) for value in coords.mean(axis=0)],
                "bbox_min_index_xyz": [int(value) for value in coords.min(axis=0)],
                "bbox_max_index_xyz": [int(value) for value in coords.max(axis=0)],
                "slice_index": int(np.median(z_values)),
                "slice_index_basis": "nifti_zero_based",
            }
        )
    components.sort(key=lambda item: (int(item["slice_index"]), int(item["component_id"])))
    return labeled, components


def _apply_liver_overlap_qc(
    labeled: np.ndarray,
    components: list[dict[str, Any]],
    liver_mask: np.ndarray,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible_components: list[dict[str, Any]] = []
    component_audit: list[dict[str, Any]] = []
    for component in components:
        component_id = int(component["component_id"])
        component_mask = labeled == component_id
        voxel_count = int(component["voxel_count"])
        overlap_voxel_count = int(np.count_nonzero(component_mask & liver_mask))
        overlap_fraction = float(overlap_voxel_count / voxel_count) if voxel_count else 0.0
        eligible = bool(overlap_voxel_count)
        component_audit.append(
            {
                "component_id": component_id,
                "voxel_count": voxel_count,
                "liver_overlap_voxel_count": overlap_voxel_count,
                "liver_overlap_fraction": overlap_fraction,
                "eligible": eligible,
                "reason": "intersects_total_liver" if eligible else "outside_total_liver",
            }
        )
        if eligible:
            eligible_component = dict(component)
            eligible_component["liver_overlap_voxel_count"] = overlap_voxel_count
            eligible_component["liver_overlap_fraction"] = overlap_fraction
            eligible_components.append(eligible_component)

    raw_voxel_count = sum(int(component["voxel_count"]) for component in components)
    eligible_voxel_count = sum(
        int(component["voxel_count"])
        for component in eligible_components
    )
    return eligible_components, {
        "method": "connected_component_intersection_with_total_liver",
        "source_mask": "artifacts/total/liver.nii.gz",
        "minimum_overlap_voxels": 1,
        "raw_component_count": len(components),
        "eligible_component_count": len(eligible_components),
        "excluded_component_count": len(components) - len(eligible_components),
        "raw_voxel_count": raw_voxel_count,
        "eligible_voxel_count": eligible_voxel_count,
        "excluded_voxel_count": raw_voxel_count - eligible_voxel_count,
        "components": component_audit,
    }


def _display_slice(array: np.ndarray, affine: np.ndarray) -> tuple[np.ndarray, tuple[str, str]]:
    axis_codes = plane_source_axis_codes(affine, "z")
    return (
        reorient_display_array(
            array,
            source_axis_codes=axis_codes,
            desired_row_code="P",
            desired_col_code="L",
        ),
        axis_codes,
    )


def render_axial_overlay_rgb(
    ct_data: np.ndarray,
    affine: np.ndarray,
    spacing_xyz: tuple[float, float, float],
    liver_mask: np.ndarray,
    lesion_mask: np.ndarray,
    *,
    slice_index: int,
    title: str,
    summary_lines: list[str],
) -> np.ndarray:
    display_ct, axis_codes = _display_slice(
        np.asarray(ct_data[:, :, slice_index], dtype=np.float32),
        affine,
    )
    display_liver, _ = _display_slice(
        np.asarray(liver_mask[:, :, slice_index], dtype=np.uint8),
        affine,
    )
    display_lesion, _ = _display_slice(
        np.asarray(lesion_mask[:, :, slice_index], dtype=np.uint8),
        affine,
    )
    display_spacing = reorient_display_spacing_mm(
        (spacing_xyz[0], spacing_xyz[1]),
        source_axis_codes=axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    aspect = display_aspect_from_spacing_mm(display_spacing)

    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(
        display_ct,
        cmap="gray",
        vmin=SOFT_TISSUE_WINDOW_LIMITS_HU[0],
        vmax=SOFT_TISSUE_WINDOW_LIMITS_HU[1],
        interpolation="nearest",
        aspect=aspect,
    )
    display_liver = np.asarray(display_liver, dtype=bool)
    display_lesion = np.asarray(display_lesion, dtype=bool)
    if display_liver.any():
        ax.contour(display_liver, levels=[0.5], colors=["#64d2ff"], linewidths=0.8)
    if display_lesion.any():
        overlay = np.ma.masked_where(~display_lesion, display_lesion.astype(np.uint8))
        ax.imshow(overlay, cmap="Reds", alpha=0.65, interpolation="nearest", aspect=aspect)
        ax.contour(display_lesion, levels=[0.5], colors=["#ff453a"], linewidths=1.2)
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
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "black", "alpha": 0.55, "edgecolor": "none"},
    )
    ax.axis("off")
    fig.tight_layout()
    fig.canvas.draw()
    rgb = np.ascontiguousarray(np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)[:, :, :3])
    plt.close(fig)
    return rgb


def main() -> int:
    args = parse_args(__doc__ or "Hepatic lesion metrics job")
    job_config = load_job_config(args.job_config_json)
    payload: dict[str, Any] = {"metric_key": METRIC_KEY, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, METRIC_KEY)
        _clear_metric_dir(metric_dir)
        locale = resolve_artifact_locale(job_config)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        lesion_path = artifacts_dir / "liver_lesions" / "liver_lesions.nii.gz"
        liver_path = artifacts_dir / "total" / "liver.nii.gz"
        payload["inputs"] = {
            "canonical_nifti": _relpath(case_dir, ct_path) if ct_path and ct_path.exists() else None,
            "liver_lesion_mask": _relpath(case_dir, lesion_path) if lesion_path.exists() else None,
            "liver_mask": _relpath(case_dir, liver_path) if liver_path.exists() else None,
        }
        if ct_path is None or not ct_path.exists() or not lesion_path.exists():
            payload.update(
                {
                    "status": "skipped",
                    "measurement": {"job_status": "missing_inputs", "has_hepatic_lesion": False},
                    "artifacts": {"result_json": _relpath(case_dir, result_path)},
                }
            )
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        shape = tuple(int(value) for value in ct_data.shape[:3])
        lesion_mask, lesion_status = _load_mask(lesion_path, shape)
        liver_mask, liver_status = _load_mask(liver_path, shape)
        spacing_xyz = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        voxel_volume_cm3 = float(np.prod(spacing_xyz) / 1000.0)
        labeled, raw_components = _label_components(lesion_mask, voxel_volume_cm3)
        components, anatomical_qc = _apply_liver_overlap_qc(
            labeled,
            raw_components,
            liver_mask,
        )
        voxel_count = sum(int(component["voxel_count"]) for component in components)
        has_lesion = bool(voxel_count)

        payload.update(
            {
                "status": "done",
                "measurement": {
                    "job_status": "complete",
                    "has_hepatic_lesion": has_lesion,
                    "notification_bool": has_lesion,
                    "lesion_voxel_count": voxel_count,
                    "lesion_component_count": len(components),
                    "lesion_total_volume_cm3": float(voxel_count * voxel_volume_cm3),
                    "components": components,
                    "anatomical_qc": anatomical_qc,
                    "lesion_mask": lesion_status,
                    "liver_mask": liver_status,
                    "total_slices": int(shape[2]),
                    "artifact_locale": locale,
                },
                "artifacts": {"result_json": _relpath(case_dir, result_path)},
            }
        )

        if has_lesion and bool(job_config.get("generate_overlay", True)):
            emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", True))
            options = secondary_capture_options_from_job_config(job_config) if emit_dicom else None
            series_uid = generate_uid() if emit_dicom else None
            if emit_dicom:
                bundle = load_case_json_bundle(args.case_id)
                case_metadata = {**bundle.get("id_json", {}), **bundle.get("metadata_json", {})}
                source_geometry = load_source_dicom_geometry(
                    case_dir,
                    series_instance_uid=str(
                        (case_metadata.get("ReferenceDicom") or {}).get("SeriesInstanceUID") or ""
                    )
                    or None,
                )
            else:
                case_metadata = None
                source_geometry = []

            overlays: list[dict[str, Any]] = []
            dicom_exports: list[dict[str, Any]] = []
            for index, component in enumerate(components, start=1):
                component_id = int(component["component_id"])
                slice_index = int(component["slice_index"])
                component_mask = labeled == component_id
                title, lines = build_component_overlay_text(
                    component_index=index,
                    component_count=len(components),
                    voxel_count=int(component["voxel_count"]),
                    volume_cm3=float(component["volume_cm3"]),
                    locale=locale,
                )
                rgb = render_axial_overlay_rgb(
                    ct_data,
                    ct_img.affine,
                    spacing_xyz,
                    liver_mask,
                    component_mask,
                    slice_index=slice_index,
                    title=title or overlay_title(locale),
                    summary_lines=lines,
                )
                png_path = metric_dir / f"lesion_component_{index:04d}.png"
                dcm_path = metric_dir / f"lesion_component_{index:04d}.dcm"
                Image.fromarray(rgb).save(png_path)
                entry: dict[str, Any] = {
                    "component_id": component_id,
                    "component_index": index,
                    "slice_index": slice_index,
                    "overlay_png": _relpath(case_dir, png_path),
                }
                component["overlay_png"] = entry["overlay_png"]
                if index == 1:
                    payload["artifacts"]["overlay_png"] = entry["overlay_png"]

                if emit_dicom and case_metadata is not None and options is not None:
                    target_position = nifti_voxel_position_lps(
                        ct_img.affine,
                        ((shape[0] - 1) / 2.0, (shape[1] - 1) / 2.0, float(slice_index)),
                    )
                    geometry = nearest_source_dicom_geometry(source_geometry, target_position)
                    if geometry is None:
                        geometry = axial_dicom_geometry_from_nifti(ct_img.affine, float(slice_index))
                    create_secondary_capture_from_rgb(
                        rgb,
                        dcm_path,
                        case_metadata,
                        series_instance_uid=series_uid,
                        series_description=series_description(locale),
                        series_number=SERIES_NUMBER,
                        instance_number=index,
                        derivation_description=derivation_description(locale),
                        **geometry,
                        slice_thickness_mm=spacing_xyz[2],
                        spacing_between_slices_mm=spacing_xyz[2],
                        max_dimension=options["max_dimension"],
                        transfer_syntax=options["transfer_syntax"],
                    )
                    entry["overlay_sc_dcm"] = _relpath(case_dir, dcm_path)
                    component["overlay_sc_dcm"] = entry["overlay_sc_dcm"]
                    dicom_exports.append({**entry, "path": entry["overlay_sc_dcm"], "kind": "secondary_capture"})
                    if index == 1:
                        payload["artifacts"]["overlay_sc_dcm"] = entry["overlay_sc_dcm"]
                overlays.append(entry)

            payload["measurement"]["components"] = components
            payload["artifacts"]["component_overlays"] = overlays
            if dicom_exports:
                payload["dicom_exports"] = dicom_exports

        write_payload(result_path, payload)
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:
        payload.update({"status": "error", "error": str(exc)})
        print(json.dumps(payload, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
