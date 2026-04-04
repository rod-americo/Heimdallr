#!/usr/bin/env python3
"""Helpers for generating DICOM Secondary Capture artifacts from RGB overlays."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    ExplicitVRLittleEndian,
    PYDICOM_IMPLEMENTATION_UID,
    SecondaryCaptureImageStorage,
    generate_uid,
)

from heimdallr.shared import settings


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
    if key in case_metadata and case_metadata.get(key) not in (None, ""):
        return case_metadata.get(key)
    reference = case_metadata.get("ReferenceDicom") or {}
    if isinstance(reference, dict):
        return reference.get(key, default)
    return default


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
) -> None:
    rgb_u8 = np.asarray(rgb, dtype=np.uint8)
    if rgb_u8.ndim != 3 or rgb_u8.shape[2] != 3:
        raise ValueError(f"RGB image must have shape (rows, cols, 3). Got {rgb_u8.shape}")

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

    study_time = str(metadata_value(case_metadata, "StudyTime", "") or "").strip()
    if study_time:
        ds.StudyTime = study_time

    ds.ContentDate = now.strftime("%Y%m%d")
    ds.ContentTime = now.strftime("%H%M%S.%f")
    ds.SeriesDate = ds.ContentDate
    ds.SeriesTime = ds.ContentTime
    ds.InstanceCreationDate = ds.ContentDate
    ds.InstanceCreationTime = ds.ContentTime
    ds.SeriesNumber = str(int(series_number))
    ds.InstanceNumber = int(instance_number)

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
    ds.save_as(str(output_path), write_like_original=False)
