#!/usr/bin/env python3
"""Helpers for generating DICOM Secondary Capture artifacts from RGB overlays."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    DeflatedExplicitVRLittleEndian,
    ExplicitVRLittleEndian,
    JPEG2000Lossless,
    JPEGLSLossless,
    PYDICOM_IMPLEMENTATION_UID,
    RLELossless,
    SecondaryCaptureImageStorage,
    generate_uid,
)

from heimdallr.shared import settings

DEFAULT_SECONDARY_CAPTURE_MAX_DIMENSION = 512
DEFAULT_SECONDARY_CAPTURE_TRANSFER_SYNTAX = "jpeg_ls_lossless"
SECONDARY_CAPTURE_TRANSFER_SYNTAXES = {
    "explicit_vr_little_endian": ExplicitVRLittleEndian,
    "original": ExplicitVRLittleEndian,
    "none": ExplicitVRLittleEndian,
    "uncompressed": ExplicitVRLittleEndian,
    str(ExplicitVRLittleEndian): ExplicitVRLittleEndian,
    "deflated_explicit_vr_little_endian": DeflatedExplicitVRLittleEndian,
    "deflated": DeflatedExplicitVRLittleEndian,
    str(DeflatedExplicitVRLittleEndian): DeflatedExplicitVRLittleEndian,
    "rle_lossless": RLELossless,
    "rle": RLELossless,
    str(RLELossless): RLELossless,
    "jpeg_ls_lossless": JPEGLSLossless,
    "jpegls_lossless": JPEGLSLossless,
    "jpeglslossless": JPEGLSLossless,
    "jpegls": JPEGLSLossless,
    str(JPEGLSLossless): JPEGLSLossless,
    "jpeg2000_lossless": JPEG2000Lossless,
    "jpeg_2000_lossless": JPEG2000Lossless,
    "jp2k_lossless": JPEG2000Lossless,
    str(JPEG2000Lossless): JPEG2000Lossless,
}


def parse_optional_float(value: Any) -> float | None:
    if value in (None, "", "Unknown"):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def resolve_study_uid(value: Any) -> str:
    raw = str(value or "").strip()
    if raw and len(raw) <= 64 and re.fullmatch(r"[0-9]+(?:\.[0-9]+)*", raw):
        return raw
    return generate_uid()


def metadata_value(case_metadata: dict[str, Any], key: str, default: Any = None) -> Any:
    reference = case_metadata.get("ReferenceDicom") or {}
    if key == "PatientName" and isinstance(reference, dict) and reference.get(key) not in (None, ""):
        return reference.get(key)
    if key in case_metadata and case_metadata.get(key) not in (None, ""):
        return case_metadata.get(key)
    if isinstance(reference, dict):
        return reference.get(key, default)
    return default


def _copy_text_attr(ds: FileDataset, case_metadata: dict[str, Any], key: str) -> None:
    value = metadata_value(case_metadata, key)
    if value in (None, ""):
        return
    text = str(value).strip()
    if text:
        setattr(ds, key, text)


def resolve_secondary_capture_transfer_syntax(value: Any):
    key = str(value or DEFAULT_SECONDARY_CAPTURE_TRANSFER_SYNTAX).strip().lower()
    transfer_syntax = SECONDARY_CAPTURE_TRANSFER_SYNTAXES.get(key)
    if transfer_syntax is None:
        allowed = ", ".join(sorted({key for key in SECONDARY_CAPTURE_TRANSFER_SYNTAXES if not key.startswith("1.")}))
        raise ValueError(f"Unsupported secondary_capture_transfer_syntax: {value!r}. Allowed values: {allowed}")
    return transfer_syntax


def secondary_capture_options_from_job_config(job_config: dict[str, Any]) -> dict[str, Any]:
    return {
        "max_dimension": int(
            job_config.get(
                "secondary_capture_max_dimension",
                DEFAULT_SECONDARY_CAPTURE_MAX_DIMENSION,
            )
            or 0
        )
        or None,
        "transfer_syntax": job_config.get(
            "secondary_capture_transfer_syntax",
            DEFAULT_SECONDARY_CAPTURE_TRANSFER_SYNTAX,
        ),
    }


def axial_dicom_geometry_from_nifti(
    affine: np.ndarray,
    slice_index: float,
) -> dict[str, Any]:
    """Return axial DICOM LPS geometry for a NIfTI voxel plane."""
    affine_array = np.asarray(affine, dtype=float)
    if affine_array.shape != (4, 4):
        raise ValueError(f"NIfTI affine must have shape (4, 4). Got {affine_array.shape}")

    ras_to_lps = np.diag([-1.0, -1.0, 1.0])
    position_ras = affine_array @ np.asarray([0.0, 0.0, float(slice_index), 1.0])
    position_lps = ras_to_lps @ position_ras[:3]

    row_lps = ras_to_lps @ affine_array[:3, 0]
    column_lps = ras_to_lps @ affine_array[:3, 1]
    row_lps /= np.linalg.norm(row_lps)
    column_lps /= np.linalg.norm(column_lps)
    normal_lps = np.cross(row_lps, column_lps)
    normal_lps /= np.linalg.norm(normal_lps)

    return {
        "image_position_patient": [float(value) for value in position_lps],
        "image_orientation_patient": [
            *[float(value) for value in row_lps],
            *[float(value) for value in column_lps],
        ],
        "slice_location": float(np.dot(position_lps, normal_lps)),
    }


def create_secondary_capture_from_rgb(
    rgb: np.ndarray,
    output_path: Path,
    case_metadata: dict[str, Any],
    *,
    series_instance_uid: str | None = None,
    series_description: str,
    series_number: int,
    instance_number: int,
    derivation_description: str,
    image_position_patient: list[float] | tuple[float, ...] | None = None,
    image_orientation_patient: list[float] | tuple[float, ...] | None = None,
    slice_location: float | None = None,
    slice_thickness_mm: float | None = None,
    spacing_between_slices_mm: float | None = None,
    max_dimension: int | None = DEFAULT_SECONDARY_CAPTURE_MAX_DIMENSION,
    transfer_syntax: Any = DEFAULT_SECONDARY_CAPTURE_TRANSFER_SYNTAX,
) -> None:
    rgb_u8 = np.asarray(rgb, dtype=np.uint8)
    if rgb_u8.ndim != 3 or rgb_u8.shape[2] != 3:
        raise ValueError(f"RGB image must have shape (rows, cols, 3). Got {rgb_u8.shape}")
    if max_dimension is not None:
        max_dimension = int(max_dimension)
        if max_dimension > 0:
            rows, cols = rgb_u8.shape[:2]
            largest = max(rows, cols)
            if largest > max_dimension:
                scale = float(max_dimension) / float(largest)
                resized = Image.fromarray(rgb_u8).resize(
                    (max(1, int(round(cols * scale))), max(1, int(round(rows * scale)))),
                    Image.Resampling.LANCZOS,
                )
                rgb_u8 = np.asarray(resized, dtype=np.uint8)

    file_meta = FileMetaDataset()
    file_meta.FileMetaInformationVersion = b"\x00\x01"
    file_meta.MediaStorageSOPClassUID = SecondaryCaptureImageStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    transfer_syntax_uid = resolve_secondary_capture_transfer_syntax(transfer_syntax)
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID

    now = settings.local_now()
    ds = FileDataset(str(output_path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.SOPClassUID = SecondaryCaptureImageStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = resolve_study_uid(metadata_value(case_metadata, "StudyInstanceUID"))
    ds.SeriesInstanceUID = series_instance_uid or generate_uid()
    ds.Modality = "OT"
    ds.SeriesDescription = series_description
    ds.ImageType = ["DERIVED", "SECONDARY"]
    ds.ConversionType = "WSD"
    ds.DerivationDescription = derivation_description
    ds.BurnedInAnnotation = "YES"
    ds.Manufacturer = "Heimdallr"
    ds.SoftwareVersions = "Heimdallr"
    ds.PatientName = str(metadata_value(case_metadata, "PatientName", "Unknown") or "Unknown")

    patient_id = str(metadata_value(case_metadata, "PatientID", "") or "").strip()
    if patient_id:
        ds.PatientID = patient_id

    _copy_text_attr(ds, case_metadata, "IssuerOfPatientID")
    _copy_text_attr(ds, case_metadata, "PatientBirthDate")
    _copy_text_attr(ds, case_metadata, "PatientBirthTime")

    patient_sex = str(metadata_value(case_metadata, "PatientSex", "") or "").strip()
    if patient_sex:
        ds.PatientSex = patient_sex
    _copy_text_attr(ds, case_metadata, "PatientAge")

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

    study_time = str(metadata_value(case_metadata, "StudyTime", "") or "").strip()
    if study_time:
        ds.StudyTime = study_time

    _copy_text_attr(ds, case_metadata, "StudyID")
    _copy_text_attr(ds, case_metadata, "StudyDescription")
    _copy_text_attr(ds, case_metadata, "InstitutionName")
    _copy_text_attr(ds, case_metadata, "InstitutionAddress")
    _copy_text_attr(ds, case_metadata, "StationName")
    _copy_text_attr(ds, case_metadata, "ReferringPhysicianName")
    _copy_text_attr(ds, case_metadata, "PerformingPhysicianName")
    _copy_text_attr(ds, case_metadata, "OperatorsName")
    _copy_text_attr(ds, case_metadata, "FrameOfReferenceUID")
    _copy_text_attr(ds, case_metadata, "BodyPartExamined")
    _copy_text_attr(ds, case_metadata, "ManufacturerModelName")

    ds.ContentDate = now.strftime("%Y%m%d")
    ds.ContentTime = now.strftime("%H%M%S.%f")
    ds.SeriesDate = ds.ContentDate
    ds.SeriesTime = ds.ContentTime
    ds.InstanceCreationDate = ds.ContentDate
    ds.InstanceCreationTime = ds.ContentTime
    ds.SeriesNumber = str(int(series_number))
    ds.InstanceNumber = int(instance_number)
    if image_position_patient is not None:
        ds.ImagePositionPatient = [float(value) for value in image_position_patient]
    if image_orientation_patient is not None:
        ds.ImageOrientationPatient = [float(value) for value in image_orientation_patient]
    if slice_location is not None:
        ds.SliceLocation = float(slice_location)
    if slice_thickness_mm is not None:
        ds.SliceThickness = float(slice_thickness_mm)
    if spacing_between_slices_mm is not None:
        ds.SpacingBetweenSlices = float(spacing_between_slices_mm)

    ds.Rows = int(rgb_u8.shape[0])
    ds.Columns = int(rgb_u8.shape[1])
    ds.SamplesPerPixel = 3
    ds.PhotometricInterpretation = "RGB"
    ds.PlanarConfiguration = 0
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelRepresentation = 0
    ds.PixelData = rgb_u8.tobytes()
    if transfer_syntax_uid == DeflatedExplicitVRLittleEndian:
        ds.file_meta.TransferSyntaxUID = DeflatedExplicitVRLittleEndian
    elif transfer_syntax_uid != ExplicitVRLittleEndian:
        ds.compress(transfer_syntax_uid)
    ds.save_as(str(output_path), write_like_original=False)
