"""Reusable outbound event payload builders for Heimdallr integrations."""

from __future__ import annotations

from typing import Any


def build_patient_identified_event(
    *,
    id_data: dict[str, Any],
    metadata_data: dict[str, Any],
    intake_manifest: dict[str, Any] | None,
) -> tuple[str, dict[str, Any]]:
    study_uid = str(id_data.get("StudyInstanceUID", "") or "").strip()
    event_key = f"patient_identified:{study_uid or id_data.get('CaseID', 'unknown')}"
    reference_dicom = metadata_data.get("ReferenceDicom", {})
    if not isinstance(reference_dicom, dict):
        reference_dicom = {}
    intake_manifest = intake_manifest if isinstance(intake_manifest, dict) else {}

    payload = {
        "event_type": "patient_identified",
        "event_version": 1,
        "event_id": event_key,
        "source": "heimdallr.prepare",
        "occurred_at": str(
            id_data.get("Pipeline", {}).get("prepare_end_time")
            or id_data.get("Pipeline", {}).get("prepare_start_time")
            or ""
        ),
        "study_instance_uid": study_uid,
        "case_id": str(id_data.get("CaseID", "") or "").strip(),
        "clinical_name": str(id_data.get("ClinicalName", "") or "").strip(),
        "accession_number": str(id_data.get("AccessionNumber", "") or "").strip(),
        "study_date": str(id_data.get("StudyDate", "") or "").strip(),
        "modality": str(id_data.get("Modality", "") or "").strip(),
        "patient_id": str(metadata_data.get("PatientID", "") or "").strip(),
        "patient_birth_date": str(metadata_data.get("PatientBirthDate", "") or "").strip(),
        "patient_sex": str(metadata_data.get("PatientSex", "") or "").strip(),
        "patient_name_display": str(metadata_data.get("PatientName", "") or "").strip(),
        "patient_name_raw": str(
            reference_dicom.get("PatientName", "")
            or metadata_data.get("PatientName", "")
            or ""
        ).strip(),
        "calling_aet": str(intake_manifest.get("calling_aet", "") or "").strip(),
        "remote_ip": str(intake_manifest.get("remote_ip", "") or "").strip(),
    }
    return event_key, payload
