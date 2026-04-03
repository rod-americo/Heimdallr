#!/usr/bin/env python3
"""Measure skeletal muscle area on the center slice of L3."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import nibabel as nib
import numpy as np
from PIL import Image
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    PYDICOM_IMPLEMENTATION_UID,
    SecondaryCaptureImageStorage,
    generate_uid,
)

from heimdallr.shared import settings
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


def parse_optional_float(value):
    if value in (None, "", "Unknown"):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def resolve_study_uid(value) -> str:
    raw = str(value or "").strip()
    if raw and len(raw) <= 64 and re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", raw):
        return raw
    return generate_uid()


def metadata_value(case_metadata: dict, key: str, default=None):
    if key in case_metadata and case_metadata.get(key) not in (None, ""):
        return case_metadata.get(key)
    reference = case_metadata.get("ReferenceDicom") or {}
    return reference.get(key, default)


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


def build_overlay(
    image_data: np.ndarray,
    l3_mask: np.ndarray,
    muscle_mask: np.ndarray,
    slice_idx: int,
    output_path: Path,
    summary_lines: list[str],
) -> None:
    ct_slice = np.asarray(image_data[:, :, slice_idx], dtype=np.float32)
    muscle_slice = np.asarray(muscle_mask[:, :, slice_idx], dtype=bool)
    l3_slice = np.asarray(l3_mask[:, :, slice_idx], dtype=bool)

    ct_slice = np.clip(ct_slice, -160.0, 240.0)
    rotated_ct = np.rot90(ct_slice)
    rotated_muscle = np.rot90(muscle_slice.astype(np.uint8))
    rotated_l3 = np.rot90(l3_slice.astype(np.uint8))

    fig, ax = plt.subplots(figsize=(8, 8))
    ax.imshow(rotated_ct, cmap="gray", interpolation="nearest")

    if rotated_muscle.any():
        muscle_overlay = np.ma.masked_where(rotated_muscle == 0, rotated_muscle)
        ax.imshow(
            muscle_overlay,
            cmap="autumn",
            interpolation="nearest",
            alpha=0.45,
            vmin=0,
            vmax=1,
        )
        ax.contour(rotated_muscle, levels=[0.5], colors=["#ffb000"], linewidths=1.2)

    if rotated_l3.any():
        ax.contour(rotated_l3, levels=[0.5], colors=["#00d5ff"], linewidths=1.0)

    ax.set_title(f"L3 Center Slice {slice_idx}", fontsize=14)
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
    fig.savefig(output_path, dpi=180, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def create_secondary_capture(
    png_path: Path,
    output_path: Path,
    case_metadata: dict,
    measurement: dict,
) -> None:
    image = Image.open(png_path).convert("RGB")
    rgb = np.asarray(image, dtype=np.uint8)

    file_meta = FileMetaDataset()
    file_meta.FileMetaInformationVersion = b"\x00\x01"
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID

    now = settings.local_now()
    ds = FileDataset(str(output_path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = resolve_study_uid(metadata_value(case_metadata, "StudyInstanceUID"))
    ds.SeriesInstanceUID = generate_uid()
    ds.Modality = "OT"
    ds.SeriesDescription = "Heimdallr L3 Muscle Area Overlay"
    ds.ImageType = ["DERIVED", "SECONDARY"]
    ds.ConversionType = "WSD"
    ds.DerivationDescription = (
        "PNG overlay burned in from Heimdallr L3 muscle area metric "
        f"(SMA={measurement['skeletal_muscle_area_cm2']:.2f} cm2"
        + (
            f", SMI={measurement['smi_cm2_m2']:.2f} cm2/m2"
            if measurement.get("smi_cm2_m2") is not None
            else ""
        )
        + ")"
    )
    ds.BurnedInAnnotation = "YES"
    ds.Manufacturer = "Heimdallr"
    ds.SoftwareVersions = "Heimdallr"
    ds.PatientName = str(metadata_value(case_metadata, "PatientName", "Unknown") or "Unknown")
    patient_id = str(metadata_value(case_metadata, "PatientID", "") or "").strip()
    if patient_id:
        ds.PatientID = patient_id
    patient_sex = str(metadata_value(case_metadata, "PatientSex", "") or "").strip()
    if patient_sex:
        ds.PatientSex = patient_sex
    patient_size = parse_optional_float(case_metadata.get("Height"))
    if patient_size is None:
        patient_size = parse_optional_float(metadata_value(case_metadata, "PatientSize"))
    if patient_size is not None:
        ds.PatientSize = f"{patient_size:.3f}"
    patient_weight = parse_optional_float(case_metadata.get("Weight"))
    if patient_weight is None:
        patient_weight = parse_optional_float(metadata_value(case_metadata, "PatientWeight"))
    if patient_weight is not None:
        ds.PatientWeight = f"{patient_weight:.1f}"
    accession_number = str(metadata_value(case_metadata, "AccessionNumber", "") or "").strip()
    if accession_number:
        ds.AccessionNumber = accession_number
    study_date = str(metadata_value(case_metadata, "StudyDate", "") or "").strip()
    if study_date:
        ds.StudyDate = study_date
    ds.ContentDate = now.strftime("%Y%m%d")
    ds.ContentTime = now.strftime("%H%M%S.%f")
    ds.SeriesDate = ds.ContentDate
    ds.SeriesTime = ds.ContentTime
    ds.InstanceCreationDate = ds.ContentDate
    ds.InstanceCreationTime = ds.ContentTime
    ds.SeriesNumber = "9101"
    ds.InstanceNumber = 1

    ds.Rows = int(rgb.shape[0])
    ds.Columns = int(rgb.shape[1])
    ds.SamplesPerPixel = 3
    ds.PhotometricInterpretation = "RGB"
    ds.PlanarConfiguration = 0
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = rgb.tobytes()
    ds.save_as(str(output_path), write_like_original=False)


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
        overlay_path = metric_dir / "overlay.png"
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

        spacing_x, spacing_y = (float(value) for value in muscle_img.header.get_zooms()[:2])
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
        if job_config.get("generate_overlay", True):
            summary_lines = [
                f"SMA: {muscle_area_cm2:.1f} cm²",
                f"NIfTI slice: {slice_idx}",
                f"Probable viewer slice: {probable_viewer_slice_index_one_based}",
            ]
            if height_m is not None:
                summary_lines.append(f"Height: {height_m:.2f} m")
            if smi_cm2_m2 is not None:
                summary_lines.append(f"SMI: {smi_cm2_m2:.1f} cm²/m²")
            build_overlay(ct_data, l3_mask, muscle_mask, slice_idx, overlay_path, summary_lines)
            artifacts["overlay_png"] = str(overlay_path.relative_to(case_dir))
            if job_config.get("emit_secondary_capture_dicom", True):
                measurement_stub = {
                    "skeletal_muscle_area_cm2": float(muscle_area_cm2),
                    "smi_cm2_m2": float(smi_cm2_m2) if smi_cm2_m2 is not None else None,
                }
                create_secondary_capture(overlay_path, overlay_sc_path, case_metadata, measurement_stub)
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
        }

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
