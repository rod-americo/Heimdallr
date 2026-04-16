#!/usr/bin/env python3
"""Helpers for generating Encapsulated PDF DICOM artifacts."""

from __future__ import annotations

from pathlib import Path

from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import (
    EncapsulatedPDFStorage,
    ExplicitVRLittleEndian,
    PYDICOM_IMPLEMENTATION_UID,
    generate_uid,
)

from heimdallr.metrics.jobs._dicom_secondary_capture import (
    metadata_value,
    parse_optional_float,
    resolve_study_uid,
)
from heimdallr.shared import settings


def create_encapsulated_pdf_dicom(
    pdf_path: Path,
    output_path: Path,
    case_metadata: dict,
    *,
    series_instance_uid: str | None = None,
    series_description: str,
    document_title: str,
    series_number: int,
    instance_number: int,
) -> None:
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF source not found: {pdf_path}")

    pdf_bytes = pdf_path.read_bytes()

    file_meta = FileMetaDataset()
    file_meta.FileMetaInformationVersion = b"\x00\x01"
    file_meta.MediaStorageSOPClassUID = EncapsulatedPDFStorage
    file_meta.MediaStorageSOPInstanceUID = generate_uid()
    file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    file_meta.ImplementationClassUID = PYDICOM_IMPLEMENTATION_UID

    now = settings.local_now()
    ds = FileDataset(str(output_path), {}, file_meta=file_meta, preamble=b"\0" * 128)
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.SOPClassUID = EncapsulatedPDFStorage
    ds.SOPInstanceUID = file_meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = resolve_study_uid(metadata_value(case_metadata, "StudyInstanceUID"))
    ds.SeriesInstanceUID = series_instance_uid or generate_uid()
    ds.Modality = "DOC"
    ds.SeriesDescription = series_description
    ds.DocumentTitle = document_title
    ds.MIMETypeOfEncapsulatedDocument = "application/pdf"
    ds.EncapsulatedDocument = pdf_bytes
    ds.BurnedInAnnotation = "NO"
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(str(output_path), write_like_original=False)
