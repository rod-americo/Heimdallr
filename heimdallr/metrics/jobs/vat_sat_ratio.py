#!/usr/bin/env python3
"""Measure VAT/SAT ratio on the center slice of L3."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np

from heimdallr.metrics.jobs._bone_job_common import (
    display_aspect_from_spacing_mm,
    plane_source_axis_codes,
    reorient_display_array,
    reorient_display_spacing_mm,
)
from heimdallr.metrics.jobs._dicom_secondary_capture import (
    create_secondary_capture_from_rgb,
    parse_optional_float,
    secondary_capture_options_from_job_config,
)
from heimdallr.metrics.jobs._vat_sat_overlay_text import (
    build_overlay_text,
    derivation_description,
    resolve_artifact_locale,
    series_description,
)
from heimdallr.metrics.analysis.bone_health import extract_study_technique_context
from heimdallr.shared.paths import study_artifacts_dir, study_dir, study_metadata_json, study_nifti


class MetricSkip(RuntimeError):
    """Signal that a metrics job should be recorded as skipped, not failed."""


SERIES_NUMBER = 9102


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", required=True, help="Study case identifier.")
    parser.add_argument("--job-config-json", default="{}", help="JSON object with job-level configuration.")
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


def _load_case_metadata(case_id: str, case_dir: Path) -> dict:
    id_json_path = case_dir / "metadata" / "id.json"
    metadata_json_path = study_metadata_json(case_id)
    merged: dict = {}
    if id_json_path.exists():
        merged.update(json.loads(id_json_path.read_text(encoding="utf-8")))
    if metadata_json_path.exists():
        merged.update(json.loads(metadata_json_path.read_text(encoding="utf-8")))
    return merged


def _selected_phase_from_metadata(case_metadata: dict) -> str:
    return str(
        (
            case_metadata.get("Pipeline", {})
            .get("series_selection", {})
            .get("SelectedPhase")
        )
        or case_metadata.get("SelectedPhase")
        or ""
    ).strip()


def compute_center_slice(mask_l3: np.ndarray) -> tuple[np.ndarray, int]:
    slice_indices = np.where(mask_l3.sum(axis=(0, 1)) > 0)[0]
    if len(slice_indices) == 0:
        raise MetricSkip("L3 mask is empty")
    center_idx = int(slice_indices[len(slice_indices) // 2])
    return slice_indices, center_idx


def sagittal_plane_from_mask(mask: np.ndarray) -> tuple[np.ndarray, int, str]:
    mask_bool = np.asarray(mask, dtype=bool)
    coords = np.argwhere(mask_bool)
    if coords.size == 0:
        raise MetricSkip("L3 mask is empty")
    x_min = int(coords[:, 0].min())
    x_max = int(coords[:, 0].max())
    center_index = int(round((x_min + x_max) / 2.0))
    return np.asarray(mask_bool[center_index, :, :], dtype=bool), center_index, "x"


def centered_slab_bounds(center_index: int, axis_len: int, spacing_mm: float, slab_thickness_mm: float) -> tuple[int, int]:
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
        projection_axis = 0
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
        projection_axis = 1
        lateral_spacing = float(spacing_mm[0])
        slab_start, slab_end = centered_slab_bounds(
            plane_index,
            image_data.shape[1],
            spacing_mm=float(spacing_mm[1]),
            slab_thickness_mm=slab_thickness_mm,
        )
        ct_slab = np.asarray(image_data[:, slab_start:slab_end, :], dtype=np.float32)
        mask_slab = np.asarray(mask[:, slab_start:slab_end, :], dtype=bool)
    sagittal_ct = np.mean(ct_slab, axis=projection_axis, dtype=np.float32)
    sagittal_mask = np.any(mask_slab, axis=projection_axis)
    return sagittal_ct, sagittal_mask, (slab_start, slab_end), lateral_spacing


def _overlay_display_directions(source_axis_codes: tuple[str, str]) -> tuple[str, str]:
    if any(code in {"A", "P"} for code in source_axis_codes):
        return "I", "P"
    if any(code in {"L", "R"} for code in source_axis_codes):
        return "I", "L"
    raise RuntimeError(f"Unsupported plane axis codes for overlay: {source_axis_codes}")


def _slice_area_cm2(mask_2d: np.ndarray, spacing_xy: tuple[float, float]) -> float:
    pixel_area_mm2 = float(spacing_xy[0] * spacing_xy[1])
    return round(float(np.count_nonzero(np.asarray(mask_2d, dtype=bool))) * pixel_area_mm2 / 100.0, 3)


def render_overlay_rgb(
    image_data: np.ndarray,
    ct_affine: np.ndarray,
    l3_mask: np.ndarray,
    sat_mask: np.ndarray,
    vat_mask: np.ndarray,
    slice_idx: int,
    title: str,
    panel_titles: tuple[str, str],
    summary_lines: list[str],
    legend_text: str,
    sagittal_level_text: str,
    spacing_mm: tuple[float, float, float],
    sagittal_slab_thickness_mm: float = 3.0,
) -> np.ndarray:
    ct_slice = np.asarray(image_data[:, :, slice_idx], dtype=np.float32)
    sat_slice = np.asarray(sat_mask[:, :, slice_idx], dtype=bool)
    vat_slice = np.asarray(vat_mask[:, :, slice_idx], dtype=bool)
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
    axial_source_axis_codes = plane_source_axis_codes(ct_affine, "z")
    rotated_ct = reorient_display_array(ct_slice, source_axis_codes=axial_source_axis_codes, desired_row_code="P", desired_col_code="L")
    rotated_sat = reorient_display_array(sat_slice.astype(np.uint8), source_axis_codes=axial_source_axis_codes, desired_row_code="P", desired_col_code="L")
    rotated_vat = reorient_display_array(vat_slice.astype(np.uint8), source_axis_codes=axial_source_axis_codes, desired_row_code="P", desired_col_code="L")
    rotated_l3 = reorient_display_array(l3_slice.astype(np.uint8), source_axis_codes=axial_source_axis_codes, desired_row_code="P", desired_col_code="L")

    sagittal_source_axis_codes = plane_source_axis_codes(ct_affine, sagittal_axis)
    sagittal_row_code, sagittal_col_code = _overlay_display_directions(sagittal_source_axis_codes)
    rotated_sagittal_ct = reorient_display_array(rotated_sagittal_ct := sagittal_ct, source_axis_codes=sagittal_source_axis_codes, desired_row_code=sagittal_row_code, desired_col_code=sagittal_col_code)
    rotated_sagittal_l3 = reorient_display_array(sagittal_l3.astype(np.uint8), source_axis_codes=sagittal_source_axis_codes, desired_row_code=sagittal_row_code, desired_col_code=sagittal_col_code)
    sagittal_level_source = np.zeros_like(sagittal_l3, dtype=bool)
    sagittal_level_source[:, slice_idx] = True
    rotated_sagittal_level = reorient_display_array(sagittal_level_source, source_axis_codes=sagittal_source_axis_codes, desired_row_code=sagittal_row_code, desired_col_code=sagittal_col_code)

    spacing_x, spacing_y, spacing_z = (float(value) for value in spacing_mm)
    axial_spacing = reorient_display_spacing_mm((spacing_x, spacing_y), source_axis_codes=axial_source_axis_codes, desired_row_code="P", desired_col_code="L")
    axial_aspect = display_aspect_from_spacing_mm(axial_spacing)
    sagittal_spacing = reorient_display_spacing_mm((lateral_spacing, spacing_z), source_axis_codes=sagittal_source_axis_codes, desired_row_code=sagittal_row_code, desired_col_code=sagittal_col_code)
    sagittal_aspect = display_aspect_from_spacing_mm(sagittal_spacing)
    slice_row_candidates = np.where(rotated_sagittal_level.any(axis=1))[0]
    slice_row = int(slice_row_candidates[len(slice_row_candidates) // 2]) if slice_row_candidates.size else int(np.clip(rotated_sagittal_ct.shape[0] // 2, 0, rotated_sagittal_ct.shape[0] - 1))

    fig, (ax_axial, ax_sagittal) = plt.subplots(
        1, 2, figsize=(12.6, 8), facecolor="black", gridspec_kw={"wspace": 0.02}
    )
    fig.patch.set_facecolor("black")
    ax_axial.set_facecolor("black")
    ax_sagittal.set_facecolor("black")
    fig.suptitle(title, fontsize=15, color="white")

    ax_axial.imshow(rotated_ct, cmap="gray", interpolation="nearest", aspect=axial_aspect)
    if rotated_sat.any():
        ax_axial.contour(rotated_sat, levels=[0.5], colors=["#4da3ff"], linewidths=1.5)
    if rotated_vat.any():
        ax_axial.contour(rotated_vat, levels=[0.5], colors=["#ffb000"], linewidths=1.5)
    if rotated_l3.any():
        ax_axial.contour(rotated_l3, levels=[0.5], colors=["#00d5ff"], linewidths=1.2)
    ax_axial.set_title(panel_titles[0], fontsize=12, color="white")
    ax_axial.text(
        0.03, 0.97, "\n".join(summary_lines), transform=ax_axial.transAxes,
        ha="left", va="top", fontsize=10, color="white",
        bbox={"boxstyle": "round,pad=0.4", "facecolor": "black", "alpha": 0.55, "edgecolor": "none"},
    )
    ax_axial.text(
        0.02, 0.02, legend_text,
        transform=ax_axial.transAxes, ha="left", va="bottom", fontsize=9, color="white"
    )
    ax_axial.axis("off")

    ax_sagittal.imshow(rotated_sagittal_ct, cmap="gray", interpolation="nearest", aspect=sagittal_aspect)
    if rotated_sagittal_l3.any():
        ax_sagittal.contour(rotated_sagittal_l3, levels=[0.5], colors=["#ffb000"], linewidths=1.1)
    ax_sagittal.axhline(slice_row, color="#00d5ff", linewidth=1.3, linestyle="--")
    ax_sagittal.text(
        0.03, 0.03, sagittal_level_text,
        transform=ax_sagittal.transAxes, ha="left", va="bottom", fontsize=9, color="white",
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "black", "alpha": 0.45, "edgecolor": "none"},
    )
    ax_sagittal.set_title(panel_titles[1], fontsize=12, color="white")
    ax_sagittal.axis("off")

    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.965), w_pad=0.15)
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.uint8)
    rgb = np.ascontiguousarray(rgba[:, :, :3])
    plt.close(fig)
    return rgb


def build_skip_payload(*, case_id: str, reason: str, result_relpath: str | None = None, inputs: dict | None = None) -> dict:
    payload = {
        "metric_key": "vat_sat_ratio",
        "status": "skipped",
        "case_id": case_id,
        "skip_reason": reason,
        "measurement": {"job_status": "skipped"},
        "artifacts": {},
        "dicom_exports": [],
    }
    if inputs:
        payload["inputs"] = inputs
    if result_relpath:
        payload["artifacts"]["result_json"] = result_relpath
    return payload


def main() -> int:
    args = parse_args()
    job_config = load_job_config(args.job_config_json)
    case_id = args.case_id
    case_dir = study_dir(case_id)
    result_relpath = "artifacts/metrics/vat_sat_ratio/result.json"
    result_path = case_dir / result_relpath
    metrics_dir = case_dir / "artifacts" / "metrics" / "vat_sat_ratio"
    overlay_path = metrics_dir / "overlay.png"
    overlay_sc_path = metrics_dir / "overlay_sc.dcm"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    artifacts_dir = study_artifacts_dir(case_id)
    ct_path = study_nifti(case_id)
    l3_path = artifacts_dir / "total" / "vertebrae_L3.nii.gz"
    l1_path = artifacts_dir / "total" / "vertebrae_L1.nii.gz"
    vat_path = artifacts_dir / "tissue_types" / "torso_fat.nii.gz"
    sat_path = artifacts_dir / "tissue_types" / "subcutaneous_fat.nii.gz"

    inputs = {
        "canonical_nifti": str(ct_path.relative_to(case_dir)) if ct_path.exists() else None,
        "vertebra_l3_mask": str(l3_path.relative_to(case_dir)) if l3_path.exists() else None,
        "vertebra_l1_mask": str(l1_path.relative_to(case_dir)) if l1_path.exists() else None,
        "visceral_fat_mask": str(vat_path.relative_to(case_dir)) if vat_path.exists() else None,
        "subcutaneous_fat_mask": str(sat_path.relative_to(case_dir)) if sat_path.exists() else None,
        "source_mask_names": {"visceral_fat": "torso_fat", "subcutaneous_fat": "subcutaneous_fat"},
    }

    try:
        if not ct_path.exists() or not l3_path.exists() or not vat_path.exists() or not sat_path.exists():
            payload = build_skip_payload(case_id=case_id, reason="missing_inputs", result_relpath=result_relpath, inputs=inputs)
            result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(json.dumps(payload, indent=2))
            return 0

        ct_img = nib.load(str(ct_path))
        image_data = ct_img.get_fdata(dtype=np.float32)
        spacing_mm = tuple(float(v) for v in ct_img.header.get_zooms()[:3])
        _, l3_mask = load_mask(l3_path)
        _, sat_mask = load_mask(sat_path)
        _, vat_mask = load_mask(vat_path)
        if not (image_data.shape == l3_mask.shape == sat_mask.shape == vat_mask.shape):
            payload = build_skip_payload(case_id=case_id, reason="shape_mismatch", result_relpath=result_relpath, inputs=inputs)
            result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            print(json.dumps(payload, indent=2))
            return 0

        slice_indices, slice_idx = compute_center_slice(l3_mask)
        sat_slice = sat_mask[:, :, slice_idx]
        vat_slice = vat_mask[:, :, slice_idx]
        pixel_spacing = (float(spacing_mm[0]), float(spacing_mm[1]))
        sat_area = _slice_area_cm2(sat_slice, pixel_spacing)
        vat_area = _slice_area_cm2(vat_slice, pixel_spacing)
        ratio = round(vat_area / sat_area, 4) if sat_area > 0 else None
        total_slices = int(image_data.shape[2])
        probable_viewer_slice = int(total_slices - slice_idx)
        artifact_locale = resolve_artifact_locale(job_config)
        title, panel_titles, summary_lines, legend_text, sagittal_level_text = build_overlay_text(
            slice_idx=slice_idx,
            probable_viewer_slice_index_one_based=probable_viewer_slice,
            sat_area_cm2=sat_area,
            vat_area_cm2=vat_area,
            ratio=ratio,
            locale=artifact_locale,
        )
        case_metadata = _load_case_metadata(case_id, case_dir)
        selected_phase = _selected_phase_from_metadata(case_metadata)
        technique = extract_study_technique_context(
            id_data=case_metadata,
            results={"SelectedPhase": selected_phase} if selected_phase else None,
        )
        artifacts = {
            "result_json": result_relpath,
        }
        dicom_exports: list[dict[str, str]] = []
        generate_overlay = bool(job_config.get("generate_overlay", True))
        emit_dicom = bool(job_config.get("emit_secondary_capture_dicom", generate_overlay))
        if generate_overlay or emit_dicom:
            rgb = render_overlay_rgb(
                image_data=image_data,
                ct_affine=ct_img.affine,
                l3_mask=l3_mask,
                sat_mask=sat_mask,
                vat_mask=vat_mask,
                slice_idx=slice_idx,
                title=title,
                panel_titles=panel_titles,
                summary_lines=summary_lines,
                legend_text=legend_text,
                sagittal_level_text=sagittal_level_text,
                spacing_mm=spacing_mm,
            )
            if generate_overlay:
                plt.imsave(overlay_path, rgb)
                artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))
            if emit_dicom:
                create_secondary_capture_from_rgb(
                    rgb,
                    overlay_sc_path,
                    case_metadata,
                    series_description=series_description(artifact_locale),
                    series_number=SERIES_NUMBER,
                    instance_number=1,
                    derivation_description=derivation_description(
                        artifact_locale,
                        vat_area_cm2=vat_area,
                        sat_area_cm2=sat_area,
                        ratio=ratio,
                    ),
                    **secondary_capture_options_from_job_config(job_config),
                )
                artifacts["overlay_sc_dcm"] = str(overlay_sc_path.relative_to(case_dir))
                dicom_exports.append(
                    {
                        "path": artifacts["overlay_sc_dcm"],
                        "kind": "secondary_capture",
                    }
                )
        payload = {
            "metric_key": "vat_sat_ratio",
            "status": "done",
            "case_id": case_id,
            "inputs": inputs,
            "measurement": {
                "job_status": "complete",
                "slice_index": int(slice_idx),
                "slice_index_basis": "nifti_zero_based",
                "probable_viewer_slice_index_one_based": probable_viewer_slice,
                "total_slices": total_slices,
                "anatomic_level_used": "L3",
                "fallback_used": False,
                "fallback_reason": None,
                "level_slice_count": int(len(slice_indices)),
                "pixel_spacing_mm": {"x": pixel_spacing[0], "y": pixel_spacing[1]},
                "source_mask_names": {"visceral_fat": "torso_fat", "subcutaneous_fat": "subcutaneous_fat"},
                "visceral_fat_pixels": int(np.count_nonzero(vat_slice)),
                "subcutaneous_fat_pixels": int(np.count_nonzero(sat_slice)),
                "pixel_area_mm2": round(float(pixel_spacing[0] * pixel_spacing[1]), 6),
                "visceral_fat_area_cm2": vat_area,
                "subcutaneous_fat_area_cm2": sat_area,
                "vat_sat_area_ratio": ratio,
                "selected_phase": selected_phase,
                "density_suppressed_due_to_contrast": bool(selected_phase and selected_phase != "native"),
                "patient_height_m": parse_optional_float(case_metadata.get("Height") or case_metadata.get("PatientSize")),
                "patient_weight_kg": parse_optional_float(case_metadata.get("Weight") or case_metadata.get("PatientWeight")),
                "study_technique": technique,
            },
            "artifacts": artifacts,
            "dicom_exports": dicom_exports,
        }
        result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0
    except MetricSkip as exc:
        payload = build_skip_payload(case_id=case_id, reason=str(exc), result_relpath=result_relpath, inputs=inputs)
        result_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0
    except Exception as exc:
        payload = {"metric_key": "vat_sat_ratio", "status": "error", "case_id": case_id, "error": str(exc)}
        print(json.dumps(payload, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
