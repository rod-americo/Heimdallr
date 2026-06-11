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
from scipy.ndimage import label as ndlabel

from heimdallr.metrics.jobs._bone_job_common import (
    load_case_json_bundle,
    load_ct_volume,
    load_job_config,
    load_nifti_mask,
    metric_output_dir,
    parse_args,
    resolve_canonical_nifti,
    write_payload,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import (
    create_secondary_capture_from_rgb,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._lung_nodules_overlay_text import (
    derivation_description,
    overlay_title,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.shared.paths import study_artifacts_dir


METRIC_KEY = "lung_nodules"
SERIES_NUMBER = 9130
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


def _component_summary(mask: np.ndarray, voxel_volume_cm3: float) -> list[dict[str, Any]]:
    mask_bool = np.asarray(mask, dtype=bool)
    if not np.any(mask_bool):
        return []
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
    return components


def _center_slice(mask: np.ndarray) -> int:
    z_indices = np.where(np.asarray(mask, dtype=bool).sum(axis=(0, 1)) > 0)[0]
    if len(z_indices) == 0:
        return int(mask.shape[2] // 2)
    return int(z_indices[len(z_indices) // 2])


def render_axial_overlay_rgb(
    ct_data: np.ndarray,
    lung_mask: np.ndarray,
    nodule_mask: np.ndarray,
    *,
    title: str,
) -> np.ndarray:
    slice_index = _center_slice(nodule_mask)
    ct_slice = np.asarray(ct_data[:, :, slice_index], dtype=np.float32)
    lung_slice = np.asarray(lung_mask[:, :, slice_index], dtype=bool)
    nodule_slice = np.asarray(nodule_mask[:, :, slice_index], dtype=bool)

    fig, ax = plt.subplots(figsize=(7, 7), facecolor="black")
    ax.set_facecolor("black")
    ax.imshow(ct_slice.T, cmap="gray", vmin=-1000.0, vmax=400.0, origin="lower", interpolation="nearest")
    if lung_slice.any():
        ax.contour(lung_slice.T, levels=[0.5], colors=["#64d2ff"], linewidths=0.8, origin="lower")
    if nodule_slice.any():
        masked = np.ma.masked_where(~nodule_slice.T, nodule_slice.T.astype(np.uint8))
        ax.imshow(masked, cmap="Reds", alpha=0.65, origin="lower", interpolation="nearest")
        ax.contour(nodule_slice.T, levels=[0.5], colors=["#ff453a"], linewidths=1.2, origin="lower")
    ax.set_title(title, fontsize=13, color="white")
    ax.axis("off")
    fig.tight_layout()
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def main() -> int:
    args = parse_args(__doc__ or "Pulmonary nodule detection summary job")
    job_config = load_job_config(args.job_config_json)
    payload: dict[str, Any] = {"metric_key": METRIC_KEY, "status": "error", "case_id": args.case_id}

    try:
        case_dir, metric_dir, result_path = metric_output_dir(args.case_id, METRIC_KEY)
        artifact_locale = resolve_artifact_locale(job_config)
        artifacts_dir = study_artifacts_dir(args.case_id)
        ct_path = resolve_canonical_nifti(args.case_id)
        nodule_dir = artifacts_dir / "lung_nodules"
        overlay_png_path = metric_dir / "overlay.png"
        overlay_sc_path = metric_dir / "overlay_sc.dcm"

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
        components = _component_summary(nodule_mask, voxel_volume_cm3)
        nodule_voxel_count = int(np.count_nonzero(nodule_mask))
        has_pulmonary_nodule = bool(nodule_voxel_count > 0)

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
            "nodule_masks": nodule_statuses,
            "lung_masks": lung_statuses,
            "lung_voxel_count": int(np.count_nonzero(lung_mask)),
            "artifact_locale": artifact_locale,
        }
        payload["artifacts"] = {
            "result_json": _relpath(case_dir, result_path),
        }

        if has_pulmonary_nodule and bool(job_config.get("generate_overlay", True)):
            rgb = render_axial_overlay_rgb(
                ct_data,
                lung_mask,
                nodule_mask,
                title=overlay_title(artifact_locale),
            )
            from PIL import Image

            Image.fromarray(rgb).save(overlay_png_path)
            payload["artifacts"]["overlay_png"] = _relpath(case_dir, overlay_png_path)

            if bool(job_config.get("emit_secondary_capture_dicom", True)):
                bundle = load_case_json_bundle(args.case_id)
                case_metadata: dict[str, Any] = {}
                case_metadata.update(bundle.get("id_json", {}))
                case_metadata.update(bundle.get("metadata_json", {}))
                options = secondary_capture_options_from_job_config(job_config)
                create_secondary_capture_from_rgb(
                    rgb,
                    overlay_sc_path,
                    case_metadata,
                    series_description=series_description(artifact_locale),
                    series_number=SERIES_NUMBER,
                    instance_number=1,
                    derivation_description=derivation_description(artifact_locale),
                    max_dimension=options["max_dimension"],
                    transfer_syntax=options["transfer_syntax"],
                )
                payload["artifacts"]["overlay_sc_dcm"] = _relpath(case_dir, overlay_sc_path)
                payload["dicom_exports"] = [
                    {
                        "path": _relpath(case_dir, overlay_sc_path),
                        "kind": "secondary_capture",
                    }
                ]

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
