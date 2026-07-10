#!/usr/bin/env python3
"""Positive-only pleural and pericardial effusion findings from TotalSegmentator masks."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from matplotlib.colors import ListedColormap
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
    create_secondary_capture_from_rgb,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._pleural_pericard_effusion_overlay_text import (
    build_component_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_artifacts_dir


METRIC_KEY = "pleural_pericard_effusion"
SERIES_NUMBER = 9131
MEDIASTINAL_WINDOW_LEVEL_HU = 40.0
MEDIASTINAL_WINDOW_WIDTH_HU = 400.0
MEDIASTINAL_WINDOW_LIMITS_HU = (
    MEDIASTINAL_WINDOW_LEVEL_HU - (MEDIASTINAL_WINDOW_WIDTH_HU / 2.0),
    MEDIASTINAL_WINDOW_LEVEL_HU + (MEDIASTINAL_WINDOW_WIDTH_HU / 2.0),
)
FINDINGS = {
    "pleural_effusion": "#32d74b",
    "pericardial_effusion": "#ff9f0a",
}
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


def _voxel_volume_cm3(image: nib.Nifti1Image) -> float:
    return float(np.prod(tuple(float(value) for value in image.header.get_zooms()[:3])) / 1000.0)


def _load_mask(path: Path, reference_shape: tuple[int, ...]) -> tuple[np.ndarray, dict[str, Any]]:
    empty = np.zeros(reference_shape, dtype=bool)
    status: dict[str, Any] = {"name": path.stem.replace(".nii", ""), "path": str(path)}
    if not path.exists():
        status.update({"status": "missing", "voxel_count": 0})
        return empty, status
    try:
        _image, mask = load_nifti_mask(path)
    except Exception as exc:
        status.update({"status": "read_error", "error": str(exc), "voxel_count": 0})
        return empty, status
    if tuple(mask.shape[:3]) != reference_shape:
        status.update(
            {
                "status": "geometry_mismatch",
                "shape": [int(value) for value in mask.shape[:3]],
                "reference_shape": [int(value) for value in reference_shape],
                "voxel_count": 0,
            }
        )
        return empty, status
    mask = np.asarray(mask, dtype=bool)
    voxel_count = int(np.count_nonzero(mask))
    status.update({"status": "present" if voxel_count else "empty", "voxel_count": voxel_count})
    return mask, status


def _load_lung_union(artifacts_dir: Path, reference_shape: tuple[int, ...]) -> np.ndarray:
    union = np.zeros(reference_shape, dtype=bool)
    for name in LUNG_MASK_NAMES:
        mask, _status = _load_mask(artifacts_dir / "total" / f"{name}.nii.gz", reference_shape)
        union |= mask
    return union


def _label_components(mask: np.ndarray, voxel_volume_cm3: float) -> tuple[np.ndarray, list[dict[str, Any]]]:
    labeled, component_count = ndlabel(
        np.asarray(mask, dtype=bool),
        structure=np.ones((3, 3, 3), dtype=np.uint8),
    )
    components: list[dict[str, Any]] = []
    for component_id in range(1, int(component_count) + 1):
        coords = np.argwhere(labeled == component_id)
        if coords.size == 0:
            continue
        voxel_count = int(coords.shape[0])
        components.append(
            {
                "component_id": component_id,
                "voxel_count": voxel_count,
                "volume_cm3": float(voxel_count * voxel_volume_cm3),
                "centroid_index_xyz": [float(value) for value in coords.mean(axis=0)],
                "bbox_min_index_xyz": [int(value) for value in coords.min(axis=0)],
                "bbox_max_index_xyz": [int(value) for value in coords.max(axis=0)],
            }
        )
    components.sort(key=lambda item: item["voxel_count"], reverse=True)
    return labeled, components


def _representative_slice(mask: np.ndarray) -> int:
    areas = np.asarray(mask, dtype=bool).sum(axis=(0, 1))
    if not np.any(areas):
        return int(mask.shape[2] // 2)
    return int(np.argmax(areas))


def _display_axial(
    array: np.ndarray,
    *,
    source_axis_codes: tuple[str, str],
) -> np.ndarray:
    return reorient_display_array(
        array,
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )


def render_overlay_rgb(
    ct_data: np.ndarray,
    ct_affine: np.ndarray,
    spacing_mm: tuple[float, float, float],
    lung_mask: np.ndarray,
    finding_mask: np.ndarray,
    *,
    color: str,
    title: str,
    summary_lines: list[str],
    slice_index: int,
) -> np.ndarray:
    source_axis_codes = plane_source_axis_codes(ct_affine, "z")
    display_ct = _display_axial(
        np.asarray(ct_data[:, :, slice_index], dtype=np.float32),
        source_axis_codes=source_axis_codes,
    )
    display_lung = _display_axial(
        np.asarray(lung_mask[:, :, slice_index], dtype=np.uint8),
        source_axis_codes=source_axis_codes,
    ).astype(bool)
    display_finding = _display_axial(
        np.asarray(finding_mask[:, :, slice_index], dtype=np.uint8),
        source_axis_codes=source_axis_codes,
    ).astype(bool)
    display_spacing = reorient_display_spacing_mm(
        (float(spacing_mm[0]), float(spacing_mm[1])),
        source_axis_codes=source_axis_codes,
        desired_row_code="P",
        desired_col_code="L",
    )
    aspect = display_aspect_from_spacing_mm(display_spacing)

    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(
        display_ct,
        cmap="gray",
        vmin=MEDIASTINAL_WINDOW_LIMITS_HU[0],
        vmax=MEDIASTINAL_WINDOW_LIMITS_HU[1],
        interpolation="nearest",
        aspect=aspect,
    )
    if display_lung.any():
        ax.contour(display_lung, levels=[0.5], colors=["#64d2ff"], linewidths=0.7)
    if display_finding.any():
        overlay = np.ma.masked_where(~display_finding, display_finding.astype(np.uint8))
        ax.imshow(overlay, cmap=ListedColormap([color]), alpha=0.62, interpolation="nearest", aspect=aspect)
        ax.contour(display_finding, levels=[0.5], colors=[color], linewidths=1.3)
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
    args = parse_args(__doc__ or "Pleural and pericardial effusion metrics job")
    job_config = load_job_config(args.job_config_json)
    payload: dict[str, Any] = {"metric_key": METRIC_KEY, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, METRIC_KEY)
        _clear_metric_dir(metric_dir)
        locale = resolve_artifact_locale(job_config)
        artifacts_dir = study_artifacts_dir(args.case_id)
        segmentation_dir = artifacts_dir / "pleural_pericard_effusion"
        ct_path = resolve_canonical_nifti(args.case_id)
        payload["inputs"] = {
            "canonical_nifti": _relpath(case_dir, ct_path) if ct_path and ct_path.exists() else None,
            "segmentation_dir": _relpath(case_dir, segmentation_dir) if segmentation_dir.exists() else None,
        }

        if ct_path is None or not ct_path.exists() or not segmentation_dir.exists():
            payload.update(
                {
                    "status": "skipped",
                    "publish_result": False,
                    "measurement": {"job_status": "missing_inputs"},
                    "artifacts": {},
                }
            )
            print(json.dumps(payload, indent=2))
            return 0

        ct_img, ct_data = load_ct_volume(ct_path)
        reference_shape = tuple(int(value) for value in ct_data.shape[:3])
        voxel_volume_cm3 = _voxel_volume_cm3(ct_img)
        lung_mask = _load_lung_union(artifacts_dir, reference_shape)
        positive_findings: dict[str, dict[str, Any]] = {}
        labeled_findings: dict[str, np.ndarray] = {}
        mask_statuses: dict[str, dict[str, Any]] = {}

        for finding in FINDINGS:
            mask, status = _load_mask(segmentation_dir / f"{finding}.nii.gz", reference_shape)
            mask_statuses[finding] = status
            voxel_count = int(np.count_nonzero(mask))
            if voxel_count == 0:
                continue
            labeled, components = _label_components(mask, voxel_volume_cm3)
            labeled_findings[finding] = labeled
            positive_findings[finding] = {
                "present": True,
                "voxel_count": voxel_count,
                "volume_cm3": float(voxel_count * voxel_volume_cm3),
                "component_count": len(components),
                "components": components,
                "source_mask": _relpath(case_dir, segmentation_dir / f"{finding}.nii.gz"),
            }

        if not positive_findings:
            payload.update(
                {
                    "status": "not_present",
                    "publish_result": False,
                    "measurement": {
                        "job_status": "complete",
                        "notification_bool": False,
                        "mask_statuses": mask_statuses,
                    },
                    "artifacts": {},
                }
            )
            print(json.dumps(payload, indent=2))
            return 0

        measurement: dict[str, Any] = {
            "job_status": "complete",
            "notification_bool": True,
            "present_findings": list(positive_findings),
            "findings": positive_findings,
            "artifact_locale": locale,
            "slice_index_basis": "nifti_zero_based",
            "probable_viewer_slice_index_basis": "one_based_reverse_z",
            "total_slices": int(reference_shape[2]),
        }
        for finding in positive_findings:
            measurement[f"has_{finding}"] = True
        payload.update(
            {
                "status": "done",
                "measurement": measurement,
                "artifacts": {"result_json": _relpath(case_dir, result_path)},
            }
        )

        if bool(job_config.get("generate_overlay", True)):
            emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", True))
            options = secondary_capture_options_from_job_config(job_config) if emit_dicom else None
            case_metadata: dict[str, Any] | None = None
            series_instance_uid = generate_uid() if emit_dicom else None
            if emit_dicom:
                bundle = load_case_json_bundle(args.case_id)
                case_metadata = {**bundle.get("id_json", {}), **bundle.get("metadata_json", {})}

            overlays: list[dict[str, Any]] = []
            dicom_exports: list[dict[str, Any]] = []
            instance_number = 0
            for finding, finding_payload in positive_findings.items():
                labeled = labeled_findings[finding]
                components = finding_payload["components"]
                for component_index, component in enumerate(components, start=1):
                    instance_number += 1
                    component_mask = labeled == int(component["component_id"])
                    slice_index = _representative_slice(component_mask)
                    viewer_slice = int(reference_shape[2] - slice_index)
                    component.update(
                        {
                            "slice_index": slice_index,
                            "probable_viewer_slice_index_one_based": viewer_slice,
                        }
                    )
                    title, lines = build_component_overlay_text(
                        finding=finding,
                        component_index=component_index,
                        component_count=len(components),
                        slice_index=slice_index,
                        probable_viewer_slice_index_one_based=viewer_slice,
                        volume_cm3=float(component["volume_cm3"]),
                        locale=locale,
                    )
                    rgb = render_overlay_rgb(
                        ct_data,
                        ct_img.affine,
                        tuple(float(value) for value in ct_img.header.get_zooms()[:3]),
                        lung_mask,
                        component_mask,
                        color=FINDINGS[finding],
                        title=title,
                        summary_lines=lines,
                        slice_index=slice_index,
                    )
                    stem = f"{finding}_component_{component_index:04d}"
                    png_path = metric_dir / f"{stem}.png"
                    dcm_path = metric_dir / f"{stem}.dcm"
                    Image.fromarray(rgb).save(png_path)
                    overlay: dict[str, Any] = {
                        "finding": finding,
                        "component_id": int(component["component_id"]),
                        "component_index": component_index,
                        "slice_index": slice_index,
                        "probable_viewer_slice_index_one_based": viewer_slice,
                        "overlay_png": _relpath(case_dir, png_path),
                    }
                    component["overlay_png"] = overlay["overlay_png"]
                    if "overlay_png" not in payload["artifacts"]:
                        payload["artifacts"]["overlay_png"] = overlay["overlay_png"]

                    if emit_dicom and case_metadata is not None and options is not None:
                        create_secondary_capture_from_rgb(
                            rgb,
                            dcm_path,
                            case_metadata,
                            series_instance_uid=series_instance_uid,
                            series_description=series_description(locale),
                            series_number=SERIES_NUMBER,
                            instance_number=instance_number,
                            derivation_description=derivation_description(locale),
                            max_dimension=options["max_dimension"],
                            transfer_syntax=options["transfer_syntax"],
                        )
                        overlay["overlay_sc_dcm"] = _relpath(case_dir, dcm_path)
                        component["overlay_sc_dcm"] = overlay["overlay_sc_dcm"]
                        dicom_exports.append(
                            {
                                "path": overlay["overlay_sc_dcm"],
                                "kind": "secondary_capture",
                                "finding": finding,
                                "component_id": int(component["component_id"]),
                            }
                        )
                        if "overlay_sc_dcm" not in payload["artifacts"]:
                            payload["artifacts"]["overlay_sc_dcm"] = overlay["overlay_sc_dcm"]
                    overlays.append(overlay)

            payload["artifacts"]["overlays"] = overlays
            if dicom_exports:
                payload["dicom_exports"] = dicom_exports

        write_payload(result_path, payload)
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:
        payload["error"] = str(exc)
        print(json.dumps(payload, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
