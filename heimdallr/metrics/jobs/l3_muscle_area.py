#!/usr/bin/env python3
"""Measure skeletal muscle area on the center slice of L3."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np

from heimdallr.metrics.jobs._dicom_secondary_capture import (
    create_secondary_capture_from_rgb,
    parse_optional_float,
)
from heimdallr.metrics.jobs._l3_overlay_text import build_overlay_text, resolve_artifact_locale
from heimdallr.shared.paths import study_artifacts_dir, study_dir, study_metadata_json, study_nifti


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True, help="Study case identifier.")
    parser.add_argument(
        "--job-config-json",
        default="{}",
        help="JSON object with job-level configuration.",
    )
    return parser.parse_args()


def load_job_config(raw_json: str) -> dict:
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid --job-config-json payload: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("Job configuration must be a JSON object")
    return parsed


def load_mask(mask_path: Path) -> tuple[nib.Nifti1Image, np.ndarray]:
    image = nib.load(str(mask_path))
    data = np.asarray(image.get_fdata())
    return image, data > 0


def compute_center_slice(mask_l3: np.ndarray) -> tuple[np.ndarray, int]:
    slice_indices = np.where(mask_l3.sum(axis=(0, 1)) > 0)[0]
    if len(slice_indices) == 0:
        raise RuntimeError("L3 mask is empty")
    center_idx = int(slice_indices[len(slice_indices) // 2])
    return slice_indices, center_idx


def sagittal_plane_from_mask(mask: np.ndarray) -> tuple[np.ndarray, int, str]:
    mask_bool = np.asarray(mask, dtype=bool)
    coords = np.argwhere(mask_bool)
    if coords.size == 0:
        raise RuntimeError("L3 mask is empty")

    x_min, y_min, _ = coords.min(axis=0)
    x_max, y_max, _ = coords.max(axis=0)
    x_span = int(x_max - x_min + 1)
    y_span = int(y_max - y_min + 1)

    if x_span <= y_span:
        center_index = int(round((x_min + x_max) / 2.0))
        return np.asarray(mask_bool[center_index, :, :], dtype=bool), center_index, "x"

    center_index = int(round((y_min + y_max) / 2.0))
    return np.asarray(mask_bool[:, center_index, :], dtype=bool), center_index, "y"


def centered_slab_bounds(center_index: int, axis_len: int, spacing_mm: float, slab_thickness_mm: float) -> tuple[int, int]:
    if axis_len <= 0:
        raise RuntimeError("Invalid slab axis length")

    slice_count = max(1, int(round(float(slab_thickness_mm) / max(float(spacing_mm), 1e-6))))
    if slice_count % 2 == 0:
        slice_count += 1

    radius = slice_count // 2
    start = max(0, center_index - radius)
    end = min(axis_len, center_index + radius + 1)

    missing = slice_count - (end - start)
    if missing > 0:
        extend_left = min(start, missing)
        start -= extend_left
        missing -= extend_left
        end = min(axis_len, end + missing)

    return int(start), int(end)


def sagittal_slab_from_mask(
    image_data: np.ndarray,
    mask: np.ndarray,
    plane_index: int,
    axis: str,
    spacing_mm: tuple[float, float, float],
    slab_thickness_mm: float,
) -> tuple[np.ndarray, np.ndarray, tuple[int, int], float]:
    if axis == "x":
        lateral_spacing = float(spacing_mm[1])
        slab_start, slab_end = centered_slab_bounds(
            plane_index,
            image_data.shape[0],
            spacing_mm=float(spacing_mm[0]),
            slab_thickness_mm=slab_thickness_mm,
        )
        ct_slab = np.asarray(image_data[slab_start:slab_end, :, :], dtype=np.float32)
        mask_slab = np.asarray(mask[slab_start:slab_end, :, :], dtype=bool)
    else:
        lateral_spacing = float(spacing_mm[0])
        slab_start, slab_end = centered_slab_bounds(
            plane_index,
            image_data.shape[1],
            spacing_mm=float(spacing_mm[1]),
            slab_thickness_mm=slab_thickness_mm,
        )
        ct_slab = np.asarray(image_data[:, slab_start:slab_end, :], dtype=np.float32)
        mask_slab = np.asarray(mask[:, slab_start:slab_end, :], dtype=bool)

    sagittal_ct = np.mean(ct_slab, axis=0, dtype=np.float32)
    sagittal_mask = np.any(mask_slab, axis=0)
    return sagittal_ct, sagittal_mask, (slab_start, slab_end), lateral_spacing


def render_overlay_rgb(
    image_data: np.ndarray,
    l3_mask: np.ndarray,
    muscle_mask: np.ndarray,
    slice_idx: int,
    title: str,
    summary_lines: list[str],
    spacing_mm: tuple[float, float, float],
    sagittal_slab_thickness_mm: float = 3.0,
) -> np.ndarray:
    ct_slice = np.asarray(image_data[:, :, slice_idx], dtype=np.float32)
    muscle_slice = np.asarray(muscle_mask[:, :, slice_idx], dtype=bool)
    l3_slice = np.asarray(l3_mask[:, :, slice_idx], dtype=bool)
    _, sagittal_index, sagittal_axis = sagittal_plane_from_mask(l3_mask)
    sagittal_ct, sagittal_l3, _, lateral_spacing = sagittal_slab_from_mask(
        image_data=image_data,
        mask=l3_mask,
        plane_index=sagittal_index,
        axis=sagittal_axis,
        spacing_mm=spacing_mm,
        slab_thickness_mm=sagittal_slab_thickness_mm,
    )

    ct_slice = np.clip(ct_slice, -160.0, 240.0)
    sagittal_ct = np.clip(sagittal_ct, -160.0, 240.0)
    rotated_ct = np.rot90(ct_slice)
    rotated_muscle = np.rot90(muscle_slice.astype(np.uint8))
    rotated_l3 = np.rot90(l3_slice.astype(np.uint8))
    rotated_sagittal_ct = np.fliplr(np.rot90(sagittal_ct))
    rotated_sagittal_l3 = np.fliplr(np.rot90(sagittal_l3.astype(np.uint8)))

    spacing_x, spacing_y, spacing_z = (float(value) for value in spacing_mm)
    axial_aspect = (spacing_y / spacing_x) if spacing_x > 0 and spacing_y > 0 else 1.0
    sagittal_aspect = (spacing_z / lateral_spacing) if spacing_z > 0 and lateral_spacing > 0 else 1.0
    slice_row = int(np.clip(sagittal_ct.shape[1] - 1 - slice_idx, 0, rotated_sagittal_ct.shape[0] - 1))

    fig, (ax_axial, ax_sagittal) = plt.subplots(1, 2, figsize=(13, 8))
    fig.suptitle(title, fontsize=15)
    ax_axial.imshow(rotated_ct, cmap="gray", interpolation="nearest", aspect=axial_aspect)

    if rotated_muscle.any():
        muscle_overlay = np.ma.masked_where(rotated_muscle == 0, rotated_muscle)
        ax_axial.imshow(
            muscle_overlay,
            cmap="autumn",
            interpolation="nearest",
            alpha=0.45,
            vmin=0,
            vmax=1,
            aspect=axial_aspect,
        )
        ax_axial.contour(rotated_muscle, levels=[0.5], colors=["#ffb000"], linewidths=1.2)

    if rotated_l3.any():
        ax_axial.contour(rotated_l3, levels=[0.5], colors=["#00d5ff"], linewidths=1.0)

    ax_axial.set_title("Axial", fontsize=12)
    ax_axial.text(
        0.03,
        0.97,
        "\n".join(summary_lines),
        transform=ax_axial.transAxes,
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
    ax_axial.axis("off")

    ax_sagittal.imshow(
        rotated_sagittal_ct,
        cmap="gray",
        interpolation="nearest",
        aspect=sagittal_aspect,
    )
    if rotated_sagittal_l3.any():
        ax_sagittal.contour(rotated_sagittal_l3, levels=[0.5], colors=["#ffb000"], linewidths=1.1)
    ax_sagittal.axhline(slice_row, color="#00d5ff", linewidth=1.3, linestyle="--")
    ax_sagittal.text(
        0.03,
        0.03,
        f"Axial level z={slice_idx} | slab {sagittal_slab_thickness_mm:.0f} mm",
        transform=ax_sagittal.transAxes,
        ha="left",
        va="bottom",
        fontsize=9,
        color="white",
        bbox={
            "boxstyle": "round,pad=0.3",
            "facecolor": "black",
            "alpha": 0.45,
            "edgecolor": "none",
        },
    )
    ax_sagittal.set_title("Sagittal Reference", fontsize=12)
    ax_sagittal.axis("off")

    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.96))
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def create_secondary_capture(
    rgb: np.ndarray,
    output_path: Path,
    case_metadata: dict,
    measurement: dict,
) -> None:
    create_secondary_capture_from_rgb(
        rgb,
        output_path,
        case_metadata,
        series_description="Heimdallr L3 Muscle Area Overlay",
        series_number=9101,
        instance_number=1,
        derivation_description=(
            "Burned-in overlay generated from Heimdallr L3 muscle area metric "
            f"(SMA={measurement['skeletal_muscle_area_cm2']:.2f} cm2"
            + (
                f", SMI={measurement['smi_cm2_m2']:.2f} cm2/m2"
                if measurement.get("smi_cm2_m2") is not None
                else ""
            )
            + ")"
        ),
    )


def main() -> int:
    args = parse_args()
    payload = {
        "metric_key": "l3_muscle_area",
        "status": "error",
        "case_id": args.case_id,
    }

    try:
        job_config = load_job_config(args.job_config_json)
        case_dir = study_dir(args.case_id)
        artifacts_dir = study_artifacts_dir(args.case_id)
        metric_dir = artifacts_dir / "metrics" / "l3_muscle_area"
        metric_dir.mkdir(parents=True, exist_ok=True)

        ct_path = study_nifti(args.case_id)
        metadata_path = study_metadata_json(args.case_id)
        metadata_source = "metadata_json"
        if not metadata_path.exists():
            metadata_path = case_dir / "metadata" / "id.json"
            metadata_source = "id_json"
        l3_path = artifacts_dir / "total" / "vertebrae_L3.nii.gz"
        muscle_path = artifacts_dir / "tissue_types" / "skeletal_muscle.nii.gz"
        result_path = metric_dir / "result.json"
        overlay_sc_path = metric_dir / "overlay_sc.dcm"

        missing = [str(path) for path in (ct_path, metadata_path, l3_path, muscle_path) if not path.exists()]
        if missing:
            raise RuntimeError(f"Required inputs not found: {missing}")
        case_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

        ct_img = nib.load(str(ct_path))
        ct_data = np.asarray(ct_img.get_fdata(), dtype=np.float32)
        _, l3_mask = load_mask(l3_path)
        muscle_img, muscle_mask = load_mask(muscle_path)

        if ct_data.shape != l3_mask.shape or ct_data.shape != muscle_mask.shape:
            raise RuntimeError(
                "Input shape mismatch between canonical CT, L3 mask, and skeletal muscle mask"
            )

        l3_slice_indices, slice_idx = compute_center_slice(l3_mask)
        muscle_slice = muscle_mask[:, :, slice_idx]
        total_slices = int(ct_data.shape[2])
        probable_viewer_slice_index_one_based = total_slices - slice_idx

        spacing_x, spacing_y, spacing_z = (float(value) for value in muscle_img.header.get_zooms()[:3])
        pixel_area_mm2 = spacing_x * spacing_y
        muscle_pixels = int(np.count_nonzero(muscle_slice))
        muscle_area_cm2 = (muscle_pixels * pixel_area_mm2) / 100.0
        height_m = parse_optional_float(case_metadata.get("Height"))
        smi_cm2_m2 = None
        height_source = None
        if height_m is not None and 0.8 <= height_m <= 2.5:
            smi_cm2_m2 = muscle_area_cm2 / (height_m**2)
            height_source = metadata_source

        center_world = nib.affines.apply_affine(
            ct_img.affine,
            np.array([ct_data.shape[0] / 2.0, ct_data.shape[1] / 2.0, float(slice_idx)]),
        )

        artifacts = {
            "result_json": str(result_path.relative_to(case_dir)),
        }
        if job_config.get("emit_secondary_capture_dicom", True):
            artifact_locale = resolve_artifact_locale(job_config)
            title, summary_lines = build_overlay_text(
                slice_idx=slice_idx,
                probable_viewer_slice_index_one_based=probable_viewer_slice_index_one_based,
                muscle_area_cm2=muscle_area_cm2,
                height_m=height_m,
                smi_cm2_m2=smi_cm2_m2,
                locale=artifact_locale,
            )
            overlay_rgb = render_overlay_rgb(
                ct_data,
                l3_mask,
                muscle_mask,
                slice_idx,
                title,
                summary_lines,
                spacing_mm=(spacing_x, spacing_y, spacing_z),
            )
            measurement_stub = {
                "skeletal_muscle_area_cm2": float(muscle_area_cm2),
                "smi_cm2_m2": float(smi_cm2_m2) if smi_cm2_m2 is not None else None,
            }
            create_secondary_capture(overlay_rgb, overlay_sc_path, case_metadata, measurement_stub)
            artifacts["overlay_sc_dcm"] = str(overlay_sc_path.relative_to(case_dir))

        payload = {
            "metric_key": "l3_muscle_area",
            "status": "done",
            "case_id": args.case_id,
            "inputs": {
                "canonical_nifti": str(ct_path.relative_to(case_dir)),
                "vertebra_l3_mask": str(l3_path.relative_to(case_dir)),
                "skeletal_muscle_mask": str(muscle_path.relative_to(case_dir)),
            },
            "measurement": {
                "slice_index": slice_idx,
                "slice_index_basis": "nifti_zero_based",
                "probable_viewer_slice_index_one_based": probable_viewer_slice_index_one_based,
                "total_slices": total_slices,
                "l3_slice_count": int(len(l3_slice_indices)),
                "muscle_pixels": muscle_pixels,
                "pixel_spacing_mm": {
                    "x": spacing_x,
                    "y": spacing_y,
                },
                "pixel_area_mm2": pixel_area_mm2,
                "skeletal_muscle_area_cm2": muscle_area_cm2,
                "height_m": height_m,
                "height_source": height_source,
                "smi_cm2_m2": smi_cm2_m2,
                "center_world_mm": {
                    "x": float(center_world[0]),
                    "y": float(center_world[1]),
                    "z": float(center_world[2]),
                },
            },
            "artifacts": artifacts,
            "dicom_exports": [],
        }
        if "overlay_sc_dcm" in artifacts:
            payload["dicom_exports"].append(
                {
                    "path": artifacts["overlay_sc_dcm"],
                    "kind": "secondary_capture",
                }
            )

        result_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        payload["error"] = str(exc)
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 1

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
