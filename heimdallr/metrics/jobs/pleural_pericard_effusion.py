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
    axial_dicom_geometry_from_nifti,
    create_secondary_capture_from_rgb,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._pleural_pericard_effusion_overlay_text import (
    build_slab_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_artifacts_dir


METRIC_KEY = "pleural_pericard_effusion"
SERIES_NUMBER = 9131
TARGET_SLICE_THICKNESS_MM = 5.0
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


def _select_slab_source_indices(
    source_positions_mm: np.ndarray,
    *,
    center_mm: float,
    slab_thickness_mm: float,
) -> list[int]:
    half_thickness = float(slab_thickness_mm) / 2.0
    selected = np.where(
        (source_positions_mm >= center_mm - half_thickness)
        & (source_positions_mm <= center_mm + half_thickness)
    )[0]
    if selected.size == 0:
        selected = np.asarray([int(np.argmin(np.abs(source_positions_mm - center_mm)))])
    return [int(index) for index in selected]


def _build_positive_slabs(
    union_mask: np.ndarray,
    *,
    spacing_z: float,
    slab_thickness_mm: float = TARGET_SLICE_THICKNESS_MM,
) -> list[dict[str, Any]]:
    occupied = np.where(np.asarray(union_mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if occupied.size == 0:
        return []
    positions = np.arange(union_mask.shape[2], dtype=np.float32) * float(spacing_z)
    center_indices = sorted(
        {
            int(np.floor((float(positions[int(index)]) / slab_thickness_mm) + 0.5))
            for index in occupied
        }
    )
    return [
        {
            "center_mm": float(center_index * slab_thickness_mm),
            "source_indices": _select_slab_source_indices(
                positions,
                center_mm=float(center_index * slab_thickness_mm),
                slab_thickness_mm=slab_thickness_mm,
            ),
        }
        for center_index in center_indices
    ]


def _average_ct_slab(ct_data: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.mean(np.asarray(ct_data[:, :, source_indices], dtype=np.float32), axis=2)


def _mask_slab(mask: np.ndarray, source_indices: list[int]) -> np.ndarray:
    return np.any(np.asarray(mask[:, :, source_indices], dtype=bool), axis=2)


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
    ct_slice: np.ndarray,
    ct_affine: np.ndarray,
    spacing_mm: tuple[float, float, float],
    lung_slice: np.ndarray,
    finding_slices: dict[str, np.ndarray],
    *,
    title: str,
    summary_lines: list[str],
) -> np.ndarray:
    source_axis_codes = plane_source_axis_codes(ct_affine, "z")
    display_ct = _display_axial(
        np.asarray(ct_slice, dtype=np.float32),
        source_axis_codes=source_axis_codes,
    )
    display_lung = _display_axial(
        np.asarray(lung_slice, dtype=np.uint8),
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
    for finding, finding_slice in finding_slices.items():
        display_finding = _display_axial(
            np.asarray(finding_slice, dtype=np.uint8),
            source_axis_codes=source_axis_codes,
        ).astype(bool)
        if not display_finding.any():
            continue
        color = FINDINGS[finding]
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
        positive_masks: dict[str, np.ndarray] = {}
        mask_statuses: dict[str, dict[str, Any]] = {}

        for finding in FINDINGS:
            mask, status = _load_mask(segmentation_dir / f"{finding}.nii.gz", reference_shape)
            mask_statuses[finding] = status
            voxel_count = int(np.count_nonzero(mask))
            if voxel_count == 0:
                continue
            _labeled, components = _label_components(mask, voxel_volume_cm3)
            positive_masks[finding] = mask
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

        union_mask = np.zeros(reference_shape, dtype=bool)
        for mask in positive_masks.values():
            union_mask |= mask
        spacing_xyz = tuple(float(value) for value in ct_img.header.get_zooms()[:3])
        export_slabs = _build_positive_slabs(
            union_mask,
            spacing_z=spacing_xyz[2],
        )

        measurement: dict[str, Any] = {
            "job_status": "complete",
            "notification_bool": True,
            "present_findings": list(positive_findings),
            "findings": positive_findings,
            "artifact_locale": locale,
            "target_slice_thickness_mm": TARGET_SLICE_THICKNESS_MM,
            "reconstruction_mode": "slab_average",
            "source_spacing_mm": {
                "x": spacing_xyz[0],
                "y": spacing_xyz[1],
                "z": spacing_xyz[2],
            },
            "exported_slice_count": len(export_slabs),
            "exported_slabs": export_slabs,
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
            for output_index, slab in enumerate(export_slabs, start=1):
                source_indices = slab["source_indices"]
                finding_slices = {
                    finding: _mask_slab(mask, source_indices)
                    for finding, mask in positive_masks.items()
                }
                present_in_slab = [
                    finding for finding, mask in finding_slices.items() if mask.any()
                ]
                if not present_in_slab:
                    continue
                title, lines = build_slab_overlay_text(
                    present_findings=present_in_slab,
                    slab_index=output_index,
                    slab_count=len(export_slabs),
                    center_mm=float(slab["center_mm"]),
                    finding_volumes_cm3={
                        finding: float(positive_findings[finding]["volume_cm3"])
                        for finding in present_in_slab
                    },
                    locale=locale,
                )
                rgb = render_overlay_rgb(
                    _average_ct_slab(ct_data, source_indices),
                    ct_img.affine,
                    spacing_xyz,
                    _mask_slab(lung_mask, source_indices),
                    finding_slices,
                    title=title,
                    summary_lines=lines,
                )
                png_path = metric_dir / f"overlay_{output_index:04d}.png"
                dcm_path = metric_dir / f"overlay_{output_index:04d}.dcm"
                Image.fromarray(rgb).save(png_path)
                overlay: dict[str, Any] = {
                    "slab_index": output_index,
                    "center_mm": float(slab["center_mm"]),
                    "source_indices": source_indices,
                    "present_findings": present_in_slab,
                    "overlay_png": _relpath(case_dir, png_path),
                }
                if "overlay_png" not in payload["artifacts"]:
                    payload["artifacts"]["overlay_png"] = overlay["overlay_png"]

                if emit_dicom and case_metadata is not None and options is not None:
                    slab_geometry = axial_dicom_geometry_from_nifti(
                        ct_img.affine,
                        float(np.mean(source_indices)),
                    )
                    create_secondary_capture_from_rgb(
                        rgb,
                        dcm_path,
                        case_metadata,
                        series_instance_uid=series_instance_uid,
                        series_description=series_description(locale),
                        series_number=SERIES_NUMBER,
                        instance_number=output_index,
                        derivation_description=derivation_description(locale),
                        **slab_geometry,
                        slice_thickness_mm=TARGET_SLICE_THICKNESS_MM,
                        spacing_between_slices_mm=TARGET_SLICE_THICKNESS_MM,
                        max_dimension=options["max_dimension"],
                        transfer_syntax=options["transfer_syntax"],
                    )
                    overlay["overlay_sc_dcm"] = _relpath(case_dir, dcm_path)
                    dicom_exports.append(
                        {
                            "path": overlay["overlay_sc_dcm"],
                            "kind": "secondary_capture",
                            "slab_index": output_index,
                            "present_findings": present_in_slab,
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
