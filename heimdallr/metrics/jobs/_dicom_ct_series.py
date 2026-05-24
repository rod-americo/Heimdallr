#!/usr/bin/env python3
"""Helpers for generating derived CT DICOM series from normalized NIfTI."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import nibabel as nib
import numpy as np
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    CTImageStorage,
    ExplicitVRLittleEndian,
    JPEGLSLossless,
    PYDICOM_IMPLEMENTATION_UID,
    generate_uid,
)

from heimdallr.metrics.jobs._dicom_secondary_capture import (
    _copy_text_attr,
    metadata_value,
    parse_optional_float,
    resolve_study_uid,
)
from heimdallr.shared import settings


RAS_TO_LPS = np.diag([-1.0, -1.0, 1.0])


def _direction(vector: np.ndarray) -> list[float]:
    norm = float(np.linalg.norm(vector))
    if norm <= 1e-6:
        raise RuntimeError("Cannot derive DICOM orientation from near-zero affine vector")
    return [float(value) for value in (np.asarray(vector, dtype=np.float64) / norm).tolist()]


def _format_decimal(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _copy_patient_study_metadata(ds: FileDataset, case_metadata: dict[str, Any]) -> None:
    ds.StudyInstanceUID = resolve_study_uid(metadata_value(case_metadata, "StudyInstanceUID"))
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

    for key in (
        "AccessionNumber",
        "StudyDate",
        "StudyTime",
        "StudyID",
        "StudyDescription",
        "InstitutionName",
        "InstitutionAddress",
        "StationName",
        "ReferringPhysicianName",
        "PerformingPhysicianName",
        "OperatorsName",
        "FrameOfReferenceUID",
        "BodyPartExamined",
        "ManufacturerModelName",
    ):
        _copy_text_attr(ds, case_metadata, key)


def _source_series_datetime(case_metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    reference = case_metadata.get("ReferenceDicom")
    if not isinstance(reference, dict):
        reference = {}

    series_date = str(reference.get("SeriesDate") or "").strip()
    series_time = str(reference.get("SeriesTime") or "").strip()
    if series_date:
        return series_date, series_time or None

    study_date = str(metadata_value(case_metadata, "StudyDate", "") or "").strip()
    study_time = str(metadata_value(case_metadata, "StudyTime", "") or "").strip()
    return study_date or None, study_time or None


def create_derived_ct_series_from_nifti(
    nifti_path: Path,
    output_dir: Path,
    case_metadata: dict[str, Any],
    *,
    series_description: str,
    series_number: int,
    preferred_display_world_ras: list[float] | tuple[float, float, float] | None = None,
    slice_thickness_mm: float | None = None,
    transfer_syntax: Any = JPEGLSLossless,
) -> list[Path]:
    """Write a derived axial CT DICOM series from a normalized NIfTI volume."""
    image = nib.load(str(nifti_path))
    data = np.asarray(image.get_fdata(), dtype=np.float32)
    if data.ndim != 3:
        raise RuntimeError(f"Expected 3D NIfTI volume. Got shape {data.shape}")

    affine = np.asarray(image.affine, dtype=np.float64)
    spacing = tuple(float(value) for value in image.header.get_zooms()[:3])
    lps_affine = affine.copy()
    lps_affine[:3, :3] = RAS_TO_LPS @ affine[:3, :3]
    lps_affine[:3, 3] = RAS_TO_LPS @ affine[:3, 3]
    row_direction = _direction(lps_affine[:3, 0])
    column_direction = _direction(lps_affine[:3, 1])
    slice_spacing = float(spacing[2])
    slice_thickness = float(slice_thickness_mm) if slice_thickness_mm is not None else slice_spacing
    if slice_thickness <= 0.0:
        raise RuntimeError("slice_thickness_mm must be positive")

    output_dir.mkdir(parents=True, exist_ok=True)
    for stale_path in output_dir.glob("normalized_geometry_*.dcm"):
        stale_path.unlink()
    series_uid = generate_uid()
    now = settings.local_now()
    source_series_date, source_series_time = _source_series_datetime(case_metadata)
    output_paths: list[Path] = []
    slice_count = int(data.shape[2])
    preferred_slice_index = None
    if preferred_display_world_ras is not None:
        preferred_world = np.asarray(
            [
                float(preferred_display_world_ras[0]),
                float(preferred_display_world_ras[1]),
                float(preferred_display_world_ras[2]),
                1.0,
            ],
            dtype=np.float64,
        )
        preferred_voxel = np.linalg.inv(affine) @ preferred_world
        preferred_slice_index = int(np.clip(round(float(preferred_voxel[2])), 0, slice_count - 1))

    export_slice_indices = list(range(slice_count))

    for export_index, slice_index in enumerate(export_slice_indices):
        pixel = np.rint(data[:, :, slice_index].T)
        pixel = np.clip(pixel, -32768, 32767).astype(np.int16)

        file_meta = FileMetaDataset()
        file_meta.FileMetaInformationVersion = b"\x00\x01"
        file_meta.MediaStorageSOPClassUID = CTImageStorage
        file_meta.MediaStorageSOPInstanceUID = generate_uid()
        file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
        file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID

        output_path = output_dir / f"normalized_geometry_{export_index + 1:04d}.dcm"
        ds = FileDataset(str(output_path), {}, file_meta=file_meta, preamble=b"\0" * 128)
        ds.is_little_endian = True
        ds.is_implicit_VR = False
        _copy_patient_study_metadata(ds, case_metadata)

        ds.SOPClassUID = CTImageStorage
        ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
        ds.SeriesInstanceUID = series_uid
        ds.Modality = "CT"
        ds.SeriesDescription = series_description
        ds.ImageType = ["DERIVED", "SECONDARY", "AXIAL"]
        ds.DerivationDescription = "Brain-mask geometry normalized axial CT"
        ds.Manufacturer = "Heimdallr"
        ds.SoftwareVersions = "Heimdallr"
        ds.ContentDate = now.strftime("%Y%m%d")
        ds.ContentTime = now.strftime("%H%M%S.%f")
        ds.SeriesDate = source_series_date or ds.ContentDate
        ds.SeriesTime = source_series_time or ds.ContentTime
        ds.InstanceCreationDate = ds.ContentDate
        ds.InstanceCreationTime = ds.ContentTime
        ds.SeriesNumber = str(int(series_number))
        ds.InstanceNumber = int(export_index + 1)
        ds.InStackPositionNumber = int(slice_index + 1)
        ds.ImagesInAcquisition = int(slice_count)
        if preferred_slice_index is not None and slice_index == preferred_slice_index:
            ds.ImageComments = "Heimdallr preferred initial display slice: brain center"

        ds.Rows = int(pixel.shape[0])
        ds.Columns = int(pixel.shape[1])
        ds.PixelSpacing = [_format_decimal(spacing[1]), _format_decimal(spacing[0])]
        ds.SliceThickness = _format_decimal(slice_thickness)
        ds.SpacingBetweenSlices = _format_decimal(slice_spacing)
        ds.ImageOrientationPatient = [
            *[_format_decimal(value) for value in row_direction],
            *[_format_decimal(value) for value in column_direction],
        ]
        ipp = lps_affine @ np.asarray([0.0, 0.0, float(slice_index), 1.0], dtype=np.float64)
        ds.ImagePositionPatient = [_format_decimal(value) for value in ipp[:3]]
        ds.SliceLocation = _format_decimal(float(np.dot(ipp[:3], _direction(lps_affine[:3, 2]))))

        ds.SamplesPerPixel = 1
        ds.PhotometricInterpretation = "MONOCHROME2"
        ds.BitsAllocated = 16
        ds.BitsStored = 16
        ds.HighBit = 15
        ds.PixelRepresentation = 1
        ds.RescaleIntercept = "0"
        ds.RescaleSlope = "1"
        ds.RescaleType = "HU"
        ds.WindowCenter = "40"
        ds.WindowWidth = "80"
        ds.PixelData = pixel.tobytes()
        ds.compress(transfer_syntax)
        ds.save_as(str(output_path), write_like_original=False)
        output_paths.append(output_path)

    return output_paths
