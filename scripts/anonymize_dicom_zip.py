#!/usr/bin/env python3
"""Create a metadata-anonymized DICOM ZIP for local smoke testing.

This utility is intentionally conservative about distribution: it writes a
local ZIP and sidecar manifest, but it does not make the output suitable for
publication. Pixel data is preserved and is not OCR-scrubbed for burned-in text.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import zipfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pydicom
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.errors import InvalidDicomError
from pydicom.tag import BaseTag, Tag
from pydicom.uid import ExplicitVRLittleEndian, generate_uid


DEFAULT_PATIENT_ID = "HEIMDALLR-SMOKE-001"
DEFAULT_PATIENT_NAME = "Heimdallr^Smoke"
DEFAULT_DATE = "20000101"
DEFAULT_TIME = "000000"
UID_PREFIX = "1.2.826.0.1.3680043.10.543."

DIRECT_IDENTIFIER_KEYWORDS = {
    "AccessionNumber",
    "AdditionalPatientHistory",
    "AdmissionID",
    "AdmittingDiagnosesDescription",
    "ConsultingPhysicianName",
    "ContentCreatorName",
    "CurrentPatientLocation",
    "DeviceSerialNumber",
    "EthnicGroup",
    "FillerOrderNumberImagingServiceRequest",
    "InstitutionAddress",
    "InstitutionName",
    "InstitutionalDepartmentName",
    "IssuerOfPatientID",
    "MedicalAlerts",
    "MilitaryRank",
    "NameOfPhysiciansReadingStudy",
    "Occupation",
    "OperatorsName",
    "OtherPatientIDs",
    "OtherPatientIDsSequence",
    "OtherPatientNames",
    "PatientAddress",
    "PatientBirthDate",
    "PatientBirthName",
    "PatientBirthTime",
    "PatientComments",
    "PatientInsurancePlanCodeSequence",
    "PatientMotherBirthName",
    "PatientPrimaryLanguageCodeSequence",
    "PatientPrimaryLanguageModifierCodeSequence",
    "PatientReligiousPreference",
    "PatientTelephoneNumbers",
    "PatientTransportArrangements",
    "PerformingPhysicianName",
    "PersonName",
    "PhysiciansOfRecord",
    "PhysiciansOfRecordIdentificationSequence",
    "PlacerOrderNumberImagingServiceRequest",
    "PregnancyStatus",
    "ReasonForStudy",
    "ReferringPhysicianAddress",
    "ReferringPhysicianName",
    "ReferringPhysicianTelephoneNumbers",
    "RegionOfResidence",
    "RequestAttributesSequence",
    "RequestedProcedureDescription",
    "RequestedProcedureID",
    "RequestingPhysician",
    "RequestingService",
    "ResponsibleOrganization",
    "ResponsiblePerson",
    "ScheduledPerformingPhysicianName",
    "StationName",
    "StudyComments",
}

PSEUDONYM_KEYWORDS = {
    "PatientID": DEFAULT_PATIENT_ID,
    "PatientName": DEFAULT_PATIENT_NAME,
    "PatientSex": "O",
}

UID_KEYWORDS_TO_KEEP = {
    "AffectedSOPClassUID",
    "ImplementationClassUID",
    "MediaStorageSOPClassUID",
    "RequestedSOPClassUID",
    "SOPClassUID",
    "TransferSyntaxUID",
}


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_archive_members(zip_handle: zipfile.ZipFile) -> Iterable[zipfile.ZipInfo]:
    for info in zip_handle.infolist():
        if info.is_dir():
            continue
        name = info.filename
        if name.startswith("/") or ".." in Path(name).parts:
            raise ValueError(f"Unsafe ZIP member path: {name}")
        yield info


def _uid_for(value: str, uid_map: dict[str, str]) -> str:
    if value not in uid_map:
        uid_map[value] = generate_uid(prefix=UID_PREFIX, entropy_srcs=[value])
    return uid_map[value]


def _replace_uid_values(ds: Dataset, uid_map: dict[str, str]) -> None:
    for elem in ds.iterall():
        if elem.VR != "UI" or elem.keyword in UID_KEYWORDS_TO_KEEP:
            continue
        if isinstance(elem.value, (list, tuple)):
            elem.value = [_uid_for(str(value), uid_map) for value in elem.value]
        elif elem.value:
            elem.value = _uid_for(str(elem.value), uid_map)


def _scrub_element(ds: Dataset, tag: BaseTag, keyword: str, vr: str) -> None:
    if keyword in PSEUDONYM_KEYWORDS:
        ds[tag].value = PSEUDONYM_KEYWORDS[keyword]
        return
    if keyword in DIRECT_IDENTIFIER_KEYWORDS:
        del ds[tag]
        return
    if vr == "PN":
        ds[tag].value = "Anonymous"
    elif vr == "DA":
        ds[tag].value = DEFAULT_DATE
    elif vr == "DT":
        ds[tag].value = f"{DEFAULT_DATE}{DEFAULT_TIME}"
    elif vr == "TM":
        ds[tag].value = DEFAULT_TIME


def _scrub_dataset(ds: Dataset) -> None:
    for elem in list(ds):
        if elem.tag.group == 0x0002:
            continue
        if elem.VR == "SQ":
            for item in elem.value:
                _scrub_dataset(item)
            continue
        _scrub_element(ds, elem.tag, elem.keyword, elem.VR)


def _scrub_metadata(ds: Dataset) -> None:
    ds.remove_private_tags()
    _scrub_dataset(ds)
    ds.PatientIdentityRemoved = "YES"
    ds.DeidentificationMethod = "Heimdallr smoke metadata deid; pixels unchanged"


def _ensure_file_meta(ds: Dataset) -> None:
    if not hasattr(ds, "file_meta") or ds.file_meta is None:
        ds.file_meta = FileMetaDataset()
    if not getattr(ds.file_meta, "TransferSyntaxUID", None):
        ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    if getattr(ds, "SOPClassUID", None):
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
    if getattr(ds, "SOPInstanceUID", None):
        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID


def anonymize_dataset(raw: bytes, uid_map: dict[str, str]) -> bytes:
    ds = pydicom.dcmread(io.BytesIO(raw), force=True)
    _replace_uid_values(ds, uid_map)
    _scrub_metadata(ds)
    _ensure_file_meta(ds)
    out = io.BytesIO()
    ds.save_as(out, write_like_original=False)
    return out.getvalue()


def _load_sample_tags(raw: bytes) -> dict[str, Any]:
    ds = pydicom.dcmread(io.BytesIO(raw), stop_before_pixels=True, force=True)
    return {
        "patient_id": str(getattr(ds, "PatientID", "")),
        "patient_name": str(getattr(ds, "PatientName", "")),
        "study_instance_uid": str(getattr(ds, "StudyInstanceUID", "")),
        "series_instance_uid": str(getattr(ds, "SeriesInstanceUID", "")),
        "sop_instance_uid": str(getattr(ds, "SOPInstanceUID", "")),
    }


def anonymize_zip(source: Path, output: Path, *, manifest: Path | None) -> dict[str, Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    uid_map: dict[str, str] = {}
    dicom_count = 0
    skipped_count = 0
    first_sample: dict[str, Any] | None = None

    tmp_output = output.with_suffix(output.suffix + ".tmp")
    if tmp_output.exists():
        tmp_output.unlink()

    with zipfile.ZipFile(source, "r") as src, zipfile.ZipFile(
        tmp_output,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as dst:
        for info in _safe_archive_members(src):
            raw = src.read(info)
            try:
                anonymized = anonymize_dataset(raw, uid_map)
            except (InvalidDicomError, AttributeError, KeyError, ValueError):
                skipped_count += 1
                continue
            dicom_count += 1
            archive_name = f"dicom/IM{dicom_count:06d}.dcm"
            dst.writestr(archive_name, anonymized)
            if first_sample is None:
                first_sample = _load_sample_tags(anonymized)

    tmp_output.replace(output)
    result = {
        "output": str(output),
        "output_size_bytes": output.stat().st_size,
        "output_sha256": _hash_file(output),
        "source_size_bytes": source.stat().st_size,
        "source_sha256": _hash_file(source),
        "dicom_instances": dicom_count,
        "skipped_non_dicom_members": skipped_count,
        "patient_id": DEFAULT_PATIENT_ID,
        "patient_name": DEFAULT_PATIENT_NAME,
        "default_date": DEFAULT_DATE,
        "pixel_data_unchanged": True,
        "distribution": "local smoke testing only; do not commit or publish",
        "sample_tags": first_sample or {},
    }
    if manifest:
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Source DICOM ZIP.")
    parser.add_argument("output", type=Path, help="Output anonymized DICOM ZIP.")
    parser.add_argument("--manifest", type=Path, help="Optional JSON sidecar manifest path.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = anonymize_zip(args.source, args.output, manifest=args.manifest)
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["dicom_instances"] == 0:
        print("No DICOM instances were written.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
