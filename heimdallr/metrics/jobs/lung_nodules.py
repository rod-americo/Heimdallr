#!/usr/bin/env python3
"""Pulmonary nodule detection summary from TotalSegmentator lung_nodules masks."""

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
    plane_source_axis_codes,
    reorient_display_array,
    reorient_display_spacing_mm,
    metric_output_dir,
    parse_args,
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
from heimdallr.metrics.jobs._lung_nodules_overlay_text import (
    build_component_overlay_text,
    derivation_description,
    overlay_title,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_artifacts_dir


METRIC_KEY = "lung_nodules"
SERIES_NUMBER = 9130
LUNG_WINDOW_LEVEL_HU = -600.0
LUNG_WINDOW_WIDTH_HU = 1500.0
LUNG_WINDOW_LIMITS_HU = (
    LUNG_WINDOW_LEVEL_HU - (LUNG_WINDOW_WIDTH_HU / 2.0),
    LUNG_WINDOW_LEVEL_HU + (LUNG_WINDOW_WIDTH_HU / 2.0),
)
LUNG_MASK_NAMES = (
    "lung_upper_lobe_left",
    "lung_upper_lobe_right",
    "lung_middle_lobe_right",
    "lung_lower_lobe_left",
    "lung_lower_lobe_right",
)


def _relpath(case_dir: Path, path: Path) -> str:
    try:
        return str(path.relative_to(case_dir))
    except ValueError:
        return str(path)


def _clear_metric_dir(metric_dir: Path) -> None:
    for path in metric_dir.iterdir():
        if path.is_file():
            path.unlink()


def _mask_voxel_volume_cm3(image: nib.Nifti1Image) -> float:
    zooms = tuple(float(value) for value in image.header.get_zooms()[:3])
    return float(np.prod(zooms) / 1000.0)


def _iter_nodule_mask_paths(nodule_dir: Path) -> list[Path]:
    if not nodule_dir.exists():
        return []
    accepted_names = {"lung_nodules.nii.gz", "lung_nodule.nii.gz", "pulmonary_nodules.nii.gz"}
    return sorted(
        path
        for path in nodule_dir.glob("*.nii.gz")
        if path.is_file() and not path.name.startswith(".") and path.name in accepted_names
    )


def _load_union_mask(mask_paths: list[Path], reference_shape: tuple[int, ...]) -> tuple[np.ndarray, list[dict[str, Any]]]:
    union = np.zeros(reference_shape, dtype=bool)
    statuses: list[dict[str, Any]] = []
    for mask_path in mask_paths:
        status: dict[str, Any] = {"name": mask_path.name, "path": str(mask_path)}
        try:
            _image, mask = load_nifti_mask(mask_path)
        except Exception as exc:
            status.update({"status": "read_error", "error": str(exc), "voxel_count": 0})
            statuses.append(status)
            continue
        if tuple(mask.shape[:3]) != reference_shape:
            status.update(
                {
                    "status": "geometry_mismatch",
                    "shape": [int(value) for value in mask.shape[:3]],
                    "reference_shape": [int(value) for value in reference_shape],
                    "voxel_count": 0,
                }
            )
            statuses.append(status)
            continue
        voxel_count = int(np.count_nonzero(mask))
        status.update({"status": "present" if voxel_count else "empty", "voxel_count": voxel_count})
        union |= mask
        statuses.append(status)
    return union, statuses


def _load_lung_union(artifacts_dir: Path, reference_shape: tuple[int, ...]) -> tuple[np.ndarray, list[dict[str, Any]]]:
    union = np.zeros(reference_shape, dtype=bool)
    statuses: list[dict[str, Any]] = []
    total_dir = artifacts_dir / "total"
    for mask_name in LUNG_MASK_NAMES:
        mask_path = total_dir / f"{mask_name}.nii.gz"
        status: dict[str, Any] = {"name": mask_name, "path": str(mask_path)}
        if not mask_path.exists():
            status.update({"status": "missing", "voxel_count": 0})
            statuses.append(status)
            continue
        try:
            _image, mask = load_nifti_mask(mask_path)
        except Exception as exc:
            status.update({"status": "read_error", "error": str(exc), "voxel_count": 0})
            statuses.append(status)
            continue
        if tuple(mask.shape[:3]) != reference_shape:
            status.update(
                {
                    "status": "geometry_mismatch",
                    "shape": [int(value) for value in mask.shape[:3]],
                    "reference_shape": [int(value) for value in reference_shape],
                    "voxel_count": 0,
                }
            )
            statuses.append(status)
            continue
        voxel_count = int(np.count_nonzero(mask))
        status.update({"status": "present" if voxel_count else "empty", "voxel_count": voxel_count})
        union |= mask
        statuses.append(status)
    return union, statuses


def _label_components(mask: np.ndarray, voxel_volume_cm3: float) -> tuple[np.ndarray, list[dict[str, Any]]]:
    mask_bool = np.asarray(mask, dtype=bool)
    if not np.any(mask_bool):
        return np.zeros(mask_bool.shape, dtype=np.int32), []
    labeled, component_count = ndlabel(mask_bool, structure=np.ones((3, 3, 3), dtype=np.uint8))
    components: list[dict[str, Any]] = []
    for component_id in range(1, int(component_count) + 1):
        coords = np.argwhere(labeled == component_id)
        voxel_count = int(coords.shape[0])
        if voxel_count <= 0:
            continue
        centroid = coords.mean(axis=0)
        components.append(
            {
                "component_id": int(component_id),
                "voxel_count": voxel_count,
                "volume_cm3": float(voxel_count * voxel_volume_cm3),
                "centroid_index_xyz": [float(value) for value in centroid],
                "bbox_min_index_xyz": [int(value) for value in coords.min(axis=0)],
                "bbox_max_index_xyz": [int(value) for value in coords.max(axis=0)],
            }
        )
    components.sort(key=lambda item: item["voxel_count"], reverse=True)
    return labeled, components


def _apply_lung_overlap_qc(
    labeled: np.ndarray,
    components: list[dict[str, Any]],
    lung_mask: np.ndarray,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    eligible_components: list[dict[str, Any]] = []
    component_audit: list[dict[str, Any]] = []
    for component in components:
        component_id = int(component["component_id"])
        component_mask = labeled == component_id
        voxel_count = int(component["voxel_count"])
        overlap_voxel_count = int(np.count_nonzero(component_mask & lung_mask))
        overlap_fraction = float(overlap_voxel_count / voxel_count) if voxel_count else 0.0
        eligible = bool(overlap_voxel_count)
        component_audit.append(
            {
                "component_id": component_id,
                "voxel_count": voxel_count,
                "lung_overlap_voxel_count": overlap_voxel_count,
                "lung_overlap_fraction": overlap_fraction,
                "eligible": eligible,
                "reason": "intersects_total_lungs" if eligible else "outside_total_lungs",
            }
        )
        if eligible:
            eligible_component = dict(component)
            eligible_component["lung_overlap_voxel_count"] = overlap_voxel_count
            eligible_component["lung_overlap_fraction"] = overlap_fraction
            eligible_components.append(eligible_component)

    raw_voxel_count = sum(int(component["voxel_count"]) for component in components)
    eligible_voxel_count = sum(
        int(component["voxel_count"])
        for component in eligible_components
    )
    return eligible_components, {
        "method": "connected_component_intersection_with_total_lungs",
        "source_masks": [f"artifacts/total/{name}.nii.gz" for name in LUNG_MASK_NAMES],
        "minimum_overlap_voxels": 1,
        "raw_component_count": len(components),
        "eligible_component_count": len(eligible_components),
        "excluded_component_count": len(components) - len(eligible_components),
        "raw_voxel_count": raw_voxel_count,
        "eligible_voxel_count": eligible_voxel_count,
        "excluded_voxel_count": raw_voxel_count - eligible_voxel_count,
        "components": component_audit,
    }


def _component_summary(mask: np.ndarray, voxel_volume_cm3: float) -> list[dict[str, Any]]:
    _labeled, components = _label_components(mask, voxel_volume_cm3)
    return components


def _center_slice(mask: np.ndarray) -> int:
    z_indices = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return int(mask.shape[2] // 2)
    return int(z_indices[len(z_indices) // 2])


def _display_axial_slices(
    ct_slice: np.ndarray,
    lung_slice: np.ndarray,
    nodule_slice: np.ndarray,
    *,
    ct_affine: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, tuple[str, str]]:
    source_axis_codes = plane_source_axis_codes(ct_affine, "z")
    return (
        reorient_display_array(
            np.asarray(ct_slice, dtype=np.float32),
            source_axis_codes=source_axis_codes,
            desired_row_code="P",
            desired_col_code="L",
        ),
        reorient_display_array(
            np.asarray(lung_slice, dtype=np.uint8),
            source_axis_codes=source_axis_codes,
            desired_row_code="P",
            desired_col_code="L",
        ).astype(bool),
        reorient_display_array(
            np.asarray(nodule_slice, dtype=np.uint8),
            source_axis_codes=source_axis_codes,
            desired_row_code="P",
            desired_col_code="L",
        ).astype(bool),
        source_axis_codes,
    )


def render_axial_overlay_rgb(
    ct_data: np.ndarray,
    ct_affine: np.ndarray,
    spacing_mm: tuple[float, float, float],
    lung_mask: np.ndarray,
    nodule_mask: np.ndarray,
    *,
    title: str,
    slice_index: int | None = None,
    summary_lines: list[str] | None = None,
) -> np.ndarray:
    if slice_index is None:
        slice_index = _center_slice(nodule_mask)
    ct_slice = np.asarray(ct_data[:, :, slice_index], dtype=np.float32)
    lung_slice = np.asarray(lung_mask[:, :, slice_index], dtype=bool)
    nodule_slice = np.asarray(nodule_mask[:, :, slice_index], dtype=bool)
    display_ct, display_lung, display_nodule, source_axis_codes = _display_axial_slices(
        ct_slice,
        lung_slice,
        nodule_slice,
        ct_affine=ct_affine,
    )
    display_spacing = reorient_display_spacing_mm(
        (float(spacing_mm[0]), float(spacing_mm[1])),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    display_aspect = display_aspect_from_spacing_mm(display_spacing)

    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(
        display_ct,
        cmap="gray",
        vmin=LUNG_WINDOW_LIMITS_HU[0],
        vmax=LUNG_WINDOW_LIMITS_HU[1],
        interpolation="nearest",
        aspect=display_aspect,
    )
    if display_lung.any():
        ax.contour(display_lung, levels=[0.5], colors=["#64d2ff"], linewidths=0.8)
    if display_nodule.any():
        masked = np.ma.masked_where(~display_nodule, display_nodule.astype(np.uint8))
        ax.imshow(masked, cmap="Reds", alpha=0.65, interpolation="nearest", aspect=display_aspect)
        ax.contour(display_nodule, levels=[0.5], colors=["#ff453a"], linewidths=1.2)
    ax.set_title(title, fontsize=13, color="white")
    if summary_lines:
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
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def _component_overlay_paths(metric_dir: Path, component_index: int) -> tuple[Path, Path]:
    stem = f"nodule_component_{component_index:04d}"
    return metric_dir / f"{stem}.png", metric_dir / f"{stem}.dcm"


def main() -> int:
    args = parse_args(__doc__ or "Pulmonary nodule detection summary job")
    job_config = load_job_config(args.job_config_json)
    payload: dict[str, Any] = {"metric_key": METRIC_KEY, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, METRIC_KEY)
        _clear_metric_dir(metric_dir)
        artifact_locale = resolve_artifact_locale(job_config)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        nodule_dir = artifacts_dir / "lung_nodules"

        payload["inputs"] = {
            "canonical_nifti": _relpath(case_dir, ct_path) if ct_path and ct_path.exists() else None,
            "lung_nodules_dir": _relpath(case_dir, nodule_dir) if nodule_dir.exists() else None,
        }

        if ct_path is None or not ct_path.exists() or not nodule_dir.exists():
            payload["status"] = "skipped"
            payload["measurement"] = {
                "job_status": "missing_inputs",
                "has_pulmonary_nodule": False,
            }
            payload["artifacts"] = {"result_json": _relpath(case_dir, result_path)}
            write_payload(result_path, payload)
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        reference_shape = tuple(int(value) for value in ct_data.shape[:3])
        nodule_paths = _iter_nodule_mask_paths(nodule_dir)
        nodule_mask, nodule_statuses = _load_union_mask(nodule_paths, reference_shape)
        lung_mask, lung_statuses = _load_lung_union(artifacts_dir, reference_shape)
        voxel_volume_cm3 = _mask_voxel_volume_cm3(ct_img)
        labeled_components, raw_components = _label_components(nodule_mask, voxel_volume_cm3)
        components, anatomical_qc = _apply_lung_overlap_qc(
            labeled_components,
            raw_components,
            lung_mask,
        )
        nodule_voxel_count = sum(int(component["voxel_count"]) for component in components)
        has_pulmonary_nodule = bool(nodule_voxel_count > 0)
        total_slices = int(reference_shape[2])

        payload["status"] = "done"
        payload["measurement"] = {
            "job_status": "complete",
            "has_pulmonary_nodule": has_pulmonary_nodule,
            "notification_bool": has_pulmonary_nodule,
            "nodule_mask_count": len(nodule_paths),
            "nodule_voxel_count": nodule_voxel_count,
            "nodule_component_count": len(components),
            "nodule_total_volume_cm3": float(nodule_voxel_count * voxel_volume_cm3),
            "components": components,
            "anatomical_qc": anatomical_qc,
            "slice_index_basis": "nifti_zero_based",
            "probable_viewer_slice_index_basis": "one_based_reverse_z",
            "total_slices": total_slices,
            "nodule_masks": nodule_statuses,
            "lung_masks": lung_statuses,
            "lung_voxel_count": int(np.count_nonzero(lung_mask)),
            "artifact_locale": artifact_locale,
        }
        payload["artifacts"] = {
            "result_json": _relpath(case_dir, result_path),
        }

        if has_pulmonary_nodule and bool(job_config.get("generate_overlay", True)):
            component_overlays: list[dict[str, Any]] = []
            dicom_exports: list[dict[str, Any]] = []
            emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", True))
            case_metadata: dict[str, Any] | None = None
            options: dict[str, Any] | None = None
            series_instance_uid = generate_uid() if emit_dicom else None
            if emit_dicom:
                bundle = load_case_json_bundle(args.case_id)
                case_metadata = {}
                case_metadata.update(bundle.get("id_json", {}))
                case_metadata.update(bundle.get("metadata_json", {}))
                options = secondary_capture_options_from_job_config(job_config)
                source_geometry = load_source_dicom_geometry(
                    case_dir,
                    series_instance_uid=str(
                        (case_metadata.get("ReferenceDicom") or {}).get("SeriesInstanceUID") or ""
                    )
                    or None,
                )
            else:
                source_geometry = []

            for component in components:
                component_id = int(component["component_id"])
                component_mask = labeled_components == component_id
                slice_index = _center_slice(component_mask)
                probable_viewer_slice = int(total_slices - slice_index)
                component["center_slice_index"] = slice_index
                component["slice_index"] = slice_index
                component["slice_index_basis"] = "nifti_zero_based"
                component["probable_viewer_slice_index_one_based"] = probable_viewer_slice
                component["total_slices"] = total_slices

            ordered_components = sorted(
                components,
                key=lambda item: (int(item["slice_index"]), int(item["component_id"])),
            )
            for component_index, component in enumerate(ordered_components, start=1):
                component_id = int(component["component_id"])
                component_mask = labeled_components == component_id
                slice_index = int(component["slice_index"])
                probable_viewer_slice = int(component["probable_viewer_slice_index_one_based"])

                title, summary_lines = build_component_overlay_text(
                    component_id=component_id,
                    component_index=component_index,
                    component_count=len(components),
                    slice_idx=slice_index,
                    probable_viewer_slice_index_one_based=probable_viewer_slice,
                    voxel_count=int(component["voxel_count"]),
                    volume_cm3=float(component["volume_cm3"]),
                    locale=artifact_locale,
                )
                rgb = render_axial_overlay_rgb(
                    ct_data,
                    ct_img.affine,
                    tuple(float(value) for value in ct_img.header.get_zooms()[:3]),
                    lung_mask,
                    component_mask,
                    title=title or overlay_title(artifact_locale),
                    slice_index=slice_index,
                    summary_lines=summary_lines,
                )
                component_png_path, component_dcm_path = _component_overlay_paths(metric_dir, component_index)
                Image.fromarray(rgb).save(component_png_path)
                component["overlay_png"] = _relpath(case_dir, component_png_path)

                overlay_entry: dict[str, Any] = {
                    "component_id": component_id,
                    "component_index": component_index,
                    "slice_index": slice_index,
                    "probable_viewer_slice_index_one_based": probable_viewer_slice,
                    "overlay_png": _relpath(case_dir, component_png_path),
                }
                if component_index == 1:
                    payload["artifacts"]["overlay_png"] = _relpath(case_dir, component_png_path)

                if emit_dicom and case_metadata is not None and options is not None:
                    target_position = nifti_voxel_position_lps(
                        ct_img.affine,
                        (
                            (reference_shape[0] - 1) / 2.0,
                            (reference_shape[1] - 1) / 2.0,
                            float(slice_index),
                        ),
                    )
                    slice_geometry = nearest_source_dicom_geometry(source_geometry, target_position)
                    if slice_geometry is None:
                        slice_geometry = axial_dicom_geometry_from_nifti(
                            ct_img.affine,
                            float(slice_index),
                        )
                    create_secondary_capture_from_rgb(
                        rgb,
                        component_dcm_path,
                        case_metadata,
                        series_instance_uid=series_instance_uid,
                        series_description=series_description(artifact_locale),
                        series_number=SERIES_NUMBER,
                        instance_number=component_index,
                        derivation_description=derivation_description(artifact_locale),
                        **slice_geometry,
                        slice_thickness_mm=float(ct_img.header.get_zooms()[2]),
                        spacing_between_slices_mm=float(ct_img.header.get_zooms()[2]),
                        max_dimension=options["max_dimension"],
                        transfer_syntax=options["transfer_syntax"],
                    )
                    component["overlay_sc_dcm"] = _relpath(case_dir, component_dcm_path)
                    overlay_entry["overlay_sc_dcm"] = _relpath(case_dir, component_dcm_path)
                    dicom_exports.append(
                        {
                            "path": _relpath(case_dir, component_dcm_path),
                            "component_id": component_id,
                            "component_index": component_index,
                            "slice_index": slice_index,
                            "probable_viewer_slice_index_one_based": probable_viewer_slice,
                            "kind": "secondary_capture",
                        }
                    )
                    if component_index == 1:
                        payload["artifacts"]["overlay_sc_dcm"] = _relpath(case_dir, component_dcm_path)

                component_overlays.append(overlay_entry)

            payload["artifacts"]["component_overlays"] = component_overlays
            payload["measurement"]["components"] = components
            if dicom_exports:
                payload["dicom_exports"] = dicom_exports

        write_payload(result_path, payload)
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:
        payload["status"] = "error"
        payload["error"] = str(exc)
        try:
            if "result_path" in locals() and result_path is not None:
                write_payload(result_path, payload)
        finally:
            print(json.dumps(payload, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
