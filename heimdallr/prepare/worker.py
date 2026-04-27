#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import argparse
import datetime
import importlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
import concurrent.futures # Adicionado para multithreading
import numpy as np
import pydicom
from pydicom.uid import JPEGLossless, JPEGLosslessSV1
from zoneinfo import ZoneInfo

# ============================================================
# CONFIGURATIONS
# ============================================================

from heimdallr.shared import settings
from heimdallr.shared import store
from heimdallr.integration_dispatcher.events import build_patient_identified_event
from heimdallr.integration_dispatcher import enqueue_dispatches
from heimdallr.shared.external_delivery import (
    delete_external_submission_sidecar,
    load_external_submission_sidecar,
    move_external_submission_sidecar,
)
from heimdallr.shared.patient_names import normalize_patient_name_display
from heimdallr.shared.paths import (
    study_artifacts_dir,
    study_derived_dir,
    study_dir,
    study_id_json,
    study_metadata_dir,
    study_metadata_json,
    study_results_json,
)
from heimdallr.shared.spool import CLAIM_SUFFIX, claim_path, unclaim_path
from heimdallr.shared.study_manifest import build_study_manifest_digest
from heimdallr.shared.sqlite import connect as db_connect

settings.configure_service_stdio()

OUTPUT_BASE_DIR = settings.OUTPUT_DIR
TOTALSEG_GET_PHASE_BIN = settings.TOTALSEG_GET_PHASE_BIN
INTAKE_MANIFEST_NAME = "_heimdallr_intake.json"
LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
JPEG_LOSSLESS_TRANSFER_SYNTAXES = {str(JPEGLossless), str(JPEGLosslessSV1)}

path_entries = [str(settings.TOTALSEG_BIN_DIR), str(Path(sys.executable).parent)]
os.environ["PATH"] = os.pathsep.join(path_entries + [os.environ["PATH"]])

settings.ensure_directories()


class PrepareError(RuntimeError):
    """Raised when a ZIP cannot be prepared into a processable study."""


def series_storage_stem(modality: str, series_number: str, description: str, uid: str) -> str:
    """Build a stable filesystem stem for persisted derived series."""
    normalized_desc = clean_filename(description.replace(" ", "_"))[:40]
    uid_tail = clean_filename(uid)[-12:] or "unknown"
    stem = f"{modality.lower()}_series_{series_number}"
    if normalized_desc:
        stem = f"{stem}_{normalized_desc}"
    return f"{stem}_{uid_tail}"


def persist_series_file(temp_path: Path, destination_path: Path) -> Path:
    """Persist a converted NIfTI or JSON sidecar into the study series folder."""
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    if destination_path.exists():
        destination_path.unlink()
    shutil.move(str(temp_path), str(destination_path))
    return destination_path

def clean_filename(s):
    s = str(s).strip()
    return re.sub(r'[^a-zA-Z0-9_-]', '', s)


def parse_optional_float(value):
    """Parse a DICOM numeric scalar into float or None."""
    if value in (None, "", "Unknown"):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def normalize_patient_name_for_prepare(name):
    """Normalize DICOM PatientName for stored metadata and case naming."""
    normalized = normalize_patient_name_display(str(name or ""), settings.PATIENT_NAME_PROFILE)
    return normalized or "Unknown"


def build_reference_dicom_context(ds):
    """Extract the subset of DICOM metadata needed by downstream derived artifacts."""
    field_names = [
        "StudyInstanceUID",
        "StudyDate",
        "StudyTime",
        "AccessionNumber",
        "PatientName",
        "PatientID",
        "PatientSex",
        "PatientBirthDate",
        "PatientAge",
        "PatientSize",
        "PatientWeight",
        "InstitutionName",
        "ReferringPhysicianName",
        "Modality",
        "SeriesInstanceUID",
        "SeriesNumber",
        "SeriesDescription",
        "SOPInstanceUID",
        "FrameOfReferenceUID",
        "KVP",
        "ConvolutionKernel",
        "SliceThickness",
        "PixelSpacing",
    ]
    context = {}
    for field_name in field_names:
        value = get_tag_value(ds, field_name, None)
        if value in (None, ""):
            continue
        if isinstance(value, (pydicom.multival.MultiValue, list, tuple)):
            context[field_name] = [str(item) for item in value]
        else:
            if field_name == "PatientName":
                context[field_name] = str(value).strip()
            else:
                context[field_name] = str(value).replace("^", " ").strip()
    return context

def generate_clinical_name(patient_name, study_date_str, accession_number):
    """
    Generates ClinicalFileName: [FirstName][Initials]_[YYYYMMDD]_[AccessionNumber]
    Example: RodrigoACS_20260131_5531196
    """
    if not patient_name or patient_name == "Unknown": return "Unknown"
    
    # Normalize name
    parts = patient_name.upper().split()
    if not parts: return "Unknown"
    
    # Filter particles (<= 3 chars)
    # Exception: First name is always kept regardless of length
    first = parts[0]
    rest = parts[1:]
    
    kept_rest = [p for p in rest if len(p) > 3]
    
    # Format
    # First name: Capitalized fully (Rodrigo)
    final_first = first.capitalize()
    
    # Initials: First char of each remaining part
    final_initials = "".join([p[0] for p in kept_rest])
    
    # Date
    if not study_date_str or len(study_date_str) < 8:
        study_date_str = "00000000"

    # Accession
    acc = str(accession_number).strip()
    if not acc: acc = "000000"
    # Remove non-alphanumeric from accession just to be safe
    acc = re.sub(r'[^a-zA-Z0-9]', '', acc)
        
    return f"{final_first}{final_initials}_{study_date_str}_{acc}"

def init_and_insert_db(metadata):
    """
    Inserts DICOM metadata into SQLite DB.
    """
    try:
        conn = db_connect()
        store.upsert_study_metadata(conn, metadata)
        conn.close()
        print(f"  [DB] Metadata saved for {metadata['StudyInstanceUID']}")
        
    except Exception as e:
        print(f"  [Error] DB Insert failed: {e}")


def enqueue_case_for_segmentation(case_id):
    """
    Enqueue the prepared study for downstream segmentation dispatch.
    """
    try:
        conn = db_connect()
        store.enqueue_segmentation_case(conn, case_id, str(study_dir(case_id)))
        conn.close()
        print(f"  [DB] Segmentation queue updated for case {case_id}")
    except Exception as e:
        print(f"  [Warning] Failed to enqueue case {case_id}: {e}")


def enqueue_patient_identified_dispatches(
    *,
    id_data: dict,
    metadata_data: dict,
    intake_manifest: dict | None,
) -> int:
    """Materialize patient-identification events into the integration outbox."""
    event_key, payload = build_patient_identified_event(
        id_data=id_data,
        metadata_data=metadata_data,
        intake_manifest=intake_manifest,
    )
    return enqueue_dispatches(
        event_type="patient_identified",
        event_version=1,
        event_key=event_key,
        case_id=str(id_data.get("CaseID", "") or "").strip() or None,
        study_uid=str(id_data.get("StudyInstanceUID", "") or "").strip() or None,
        payload=payload,
    )


def _read_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _read_study_json_if_matching(path: Path, study_uid: str) -> dict:
    payload = _read_json_dict(path)
    if str(payload.get("StudyInstanceUID", "") or "") != str(study_uid or ""):
        return {}
    return payload


def _delete_spooled_zip(zip_path: Path) -> None:
    try:
        if zip_path.exists():
            zip_path.unlink()
            print(f"  Deleted input ZIP: {zip_path}")
        delete_external_submission_sidecar(zip_path)
    except Exception as exc:
        print(f"  Warning: Could not delete input ZIP: {exc}")


def _extract_manifest_fingerprint(intake_manifest: dict) -> tuple[str, str]:
    study_uid = str(intake_manifest.get("study_uid", "") or "").strip()
    manifest_digest = str(intake_manifest.get("manifest_digest", "") or "").strip()
    return study_uid, manifest_digest


def _load_intake_manifest_from_zip(zip_path: Path) -> dict:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            try:
                with zf.open(INTAKE_MANIFEST_NAME, "r") as manifest_file:
                    payload = json.load(manifest_file)
            except KeyError:
                return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _existing_case_ready_for_duplicate_prepare(case_id: str, study_uid: str) -> bool:
    return bool(_read_study_json_if_matching(study_id_json(case_id), study_uid))


def _should_skip_duplicate_prepare_manifest(
    *,
    study_uid: str,
    manifest_digest: str,
    case_id: str,
) -> bool:
    if not study_uid or not manifest_digest:
        return False
    conn = db_connect()
    try:
        existing_handoff = store.get_study_handoff_state(conn, study_uid, manifest_digest)
        if not existing_handoff:
            return False
        if not store.is_suppressed_study_handoff_status(existing_handoff["status"]):
            return False
        existing_case_id = str(existing_handoff["case_id"] or case_id or "").strip() or case_id
        return _existing_case_ready_for_duplicate_prepare(existing_case_id, study_uid)
    finally:
        conn.close()


def _build_prepare_output_payloads(
    *,
    case_id: str,
    id_data: dict,
    metadata_data: dict,
    available_series: list[dict],
    discarded_series: list[dict],
    prepare_pipeline_updates: dict,
    duplicate_skip_context: dict | None,
    reference_dicom_context: dict | None,
) -> tuple[dict, dict]:
    study_uid = str(id_data.get("StudyInstanceUID", "") or "")
    latest_id_data = _read_study_json_if_matching(study_id_json(case_id), study_uid)
    latest_metadata_data = _read_study_json_if_matching(
        study_metadata_json(case_id),
        str(metadata_data.get("StudyInstanceUID", "") or ""),
    )

    output_meta = latest_id_data.copy()
    output_meta.update(id_data)

    output_metadata = latest_metadata_data.copy()
    output_metadata.update(metadata_data)
    if reference_dicom_context:
        output_metadata["ReferenceDicom"] = reference_dicom_context

    pipeline_data = output_meta.get("Pipeline", {})
    if not isinstance(pipeline_data, dict):
        pipeline_data = {}
    else:
        pipeline_data = dict(pipeline_data)
    pipeline_data.update(prepare_pipeline_updates)

    if duplicate_skip_context:
        pipeline_data["series_selection"] = duplicate_skip_context["selection_info"]
        pipeline_data["segmentation_skipped"] = True
        pipeline_data["segmentation_skip_reason"] = duplicate_skip_context["segmentation_skip_reason"]
        pipeline_data["segmentation_skip_time"] = prepare_pipeline_updates["prepare_end_time"]
        pipeline_data["metrics_profile"] = duplicate_skip_context["metrics_profile"]
        pipeline_data["segmentation_pipeline"] = {
            "profile": duplicate_skip_context["segmentation_profile"],
            "tasks": duplicate_skip_context["segmentation_tasks"],
            "reused_existing_outputs": True,
            "reuse_reason": duplicate_skip_context["segmentation_reuse_reason"],
        }
        if duplicate_skip_context.get("segmentation_original_elapsed_time"):
            pipeline_data["segmentation_original_elapsed_time"] = duplicate_skip_context[
                "segmentation_original_elapsed_time"
            ]

    output_meta["Pipeline"] = pipeline_data
    output_meta["AvailableSeries"] = available_series
    output_meta["DiscardedSeries"] = discarded_series
    return output_meta, output_metadata


def _safe_int(value, default=-1):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _select_prepared_series_for_duplicate_check(case_id: str, id_data: dict):
    from heimdallr.segmentation.worker import select_prepared_series

    return select_prepared_series(case_id, id_data)


def _resolve_segmentation_plan_for_duplicate_check(modality: str, selected_phase: str):
    from heimdallr.segmentation.worker import resolve_segmentation_plan

    return resolve_segmentation_plan(modality, selected_phase)


def _segmentation_outputs_exist_for_duplicate_check(case_output: Path, tasks: list[dict]) -> bool:
    for task in tasks:
        output_dir = case_output / task["output_dir"]
        if not output_dir.exists():
            return False
        try:
            next(output_dir.iterdir())
        except StopIteration:
            return False
    return True


def _load_metrics_pipeline_profile_for_duplicate_check():
    from heimdallr.metrics.worker import load_metrics_pipeline_profile

    return load_metrics_pipeline_profile()


def _completed_case_skip_context(case_id: str, id_data: dict) -> dict | None:
    """Return duplicate-completion context when a case can skip segmentation."""
    study_uid = str(id_data.get("StudyInstanceUID", "") or "").strip()
    if not study_uid:
        return None

    try:
        _, selection_info = _select_prepared_series_for_duplicate_check(case_id, id_data)
        segmentation_profile, segmentation_tasks = _resolve_segmentation_plan_for_duplicate_check(
            str(id_data.get("Modality", "") or "CT"),
            str(selection_info.get("SelectedPhase", "") or "unknown"),
        )
        metrics_profile, _ = _load_metrics_pipeline_profile_for_duplicate_check()
    except Exception:
        return None

    conn = db_connect()
    try:
        recorded = store.get_pipeline_completion_state(conn, study_uid)
        if not recorded:
            return None
        if not recorded["SegmentationCompletedAt"]:
            return None
        if bool(recorded["ArtifactsPurged"]):
            return None
        metrics_statuses = store.get_case_metrics_queue_statuses(conn, case_id)
        egress_statuses = store.get_case_dicom_egress_statuses(conn, case_id)
        metrics_completed = bool(recorded["MetricsCompletedAt"])
        metrics_has_active_or_done_queue = bool(
            metrics_statuses.intersection({"pending", "claimed", "done"})
        )
        if not metrics_completed and not metrics_has_active_or_done_queue:
            if not study_results_json(case_id).exists():
                return None
            if store.case_has_incomplete_metrics(conn, case_id):
                return None
        recorded_metrics_profile = str(recorded["MetricsProfile"] or "")
        if recorded_metrics_profile and recorded_metrics_profile != metrics_profile:
            return None
        if str(recorded["SegmentationSeriesInstanceUID"] or "") != str(
            selection_info.get("SelectedSeriesInstanceUID") or ""
        ):
            return None
        if _safe_int(recorded["SegmentationSliceCount"]) != _safe_int(
            selection_info.get("SliceCount")
        ):
            return None
        if str(recorded["SegmentationProfile"] or "") != segmentation_profile:
            return None
        try:
            recorded_tasks = json.loads(recorded["SegmentationTasks"] or "[]")
        except Exception:
            recorded_tasks = []
        planned_task_names = [task["name"] for task in segmentation_tasks]
        if recorded_tasks != planned_task_names:
            return None
    finally:
        conn.close()

    if not study_results_json(case_id).exists() and not metrics_has_active_or_done_queue:
        return None
    if not _segmentation_outputs_exist_for_duplicate_check(study_dir(case_id), segmentation_tasks):
        return None

    skip_reason = "previous_pipeline_complete_signature_match"
    reuse_reason = "prepare_duplicate_complete"
    if metrics_statuses.intersection({"pending", "claimed"}):
        skip_reason = "matching_segmentation_metrics_in_progress_signature_match"
        reuse_reason = "prepare_duplicate_metrics_in_progress"
    elif egress_statuses.intersection({"pending", "claimed"}):
        skip_reason = "matching_segmentation_egress_in_progress_signature_match"
        reuse_reason = "prepare_duplicate_egress_in_progress"
    elif egress_statuses.intersection({"error"}):
        skip_reason = "matching_segmentation_egress_error_signature_match"
        reuse_reason = "prepare_duplicate_egress_error"
    elif metrics_statuses.intersection({"done"}) or metrics_completed:
        skip_reason = "matching_segmentation_metrics_done_signature_match"
        reuse_reason = "prepare_duplicate_metrics_done"

    return {
        "selection_info": selection_info,
        "segmentation_profile": segmentation_profile,
        "segmentation_skip_reason": skip_reason,
        "segmentation_reuse_reason": reuse_reason,
        "segmentation_original_elapsed_time": str(recorded["SegmentationElapsedTime"] or "") or None,
        "segmentation_tasks": [
            {
                "name": task["name"],
                "output_dir": task["output_dir"],
                "extra_args": list(task.get("extra_args", [])),
                "license_required": bool(task.get("license_required")),
            }
            for task in segmentation_tasks
        ],
        "metrics_profile": metrics_profile,
    }


def is_spooled_zip_stable(zip_path: Path, min_age_seconds: int | None = None) -> bool:
    """Return True when a staged ZIP is old enough to be safely claimed."""
    if min_age_seconds is None:
        min_age_seconds = settings.PREPARE_STABLE_AGE_SECONDS
    try:
        stat = zip_path.stat()
    except FileNotFoundError:
        return False
    age_seconds = time.time() - stat.st_mtime
    return age_seconds >= min_age_seconds


def move_failed_upload(zip_path: Path) -> Path:
    """Move a claimed upload ZIP to the failed spool, preserving the base name."""
    failed_base = unclaim_path(zip_path).name
    destination = settings.UPLOAD_FAILED_DIR / failed_base
    if destination.exists():
        destination = settings.UPLOAD_FAILED_DIR / f"{destination.stem}_{int(time.time())}{destination.suffix}"
    zip_path.replace(destination)
    move_external_submission_sidecar(zip_path, destination)
    return destination

def extract_full_dicom_metadata(ds):
    """
    Extracts all standard DICOM tags into a dictionary.
    Excludes Pixel Data and long binary fields.
    """
    meta = {}
    for elem in ds:
        if elem.tag.group == 0x7FE0: continue # Skip Pixel Data
        keyword = elem.keyword
        if not keyword: continue
        
        val = elem.value
        # Handle types
        if isinstance(val, (pydicom.multival.MultiValue, list, tuple)):
            val = [str(x) for x in val]
        elif isinstance(val, (bytes, bytearray)):
             val = "<binary>"
        else:
             val = str(val)
             
        meta[keyword] = val
    return meta

def update_db_full_metadata(study_uid, full_meta):
    try:
        conn = db_connect()
        store.update_full_dicom_metadata(conn, study_uid, full_meta)
        conn.close()
        print(f"  [DB] Full DICOM Metadata updated for {study_uid}")
    except Exception as e:
        print(f"  [Error] DB Update Full Metadata failed: {e}")


def update_db_biometrics(study_uid, *, weight=None, height=None):
    try:
        conn = db_connect()
        store.update_study_biometrics(conn, study_uid, weight=weight, height=height)
        conn.close()
    except Exception as e:
        print(f"  [Error] DB Update Biometrics failed: {e}")

def get_tag_value(ds, tag, default=None):
    return getattr(ds, tag, default)


def split_series_by_image_count(series_map, min_images):
    """Split detected series into eligible and discarded groups by image count."""
    eligible = {}
    discarded = []
    for uid, series_data in series_map.items():
        image_count = len(series_data.get("files", []))
        if image_count < min_images:
            discarded.append(
                {
                    "SeriesInstanceUID": uid,
                    "SeriesNumber": series_data.get("SeriesNumber", ""),
                    "Modality": series_data.get("Modality", ""),
                    "SeriesDescription": series_data.get("SeriesDescriptionOriginal", ""),
                    "ImageCount": image_count,
                    "DiscardReason": f"below_min_images_{min_images}",
                }
            )
            continue
        eligible[uid] = series_data
    return eligible, discarded

def process_ct_series_concurrency(uid, s_data, case_output_dir, temp_dir):
    """
    Helper function to process a single CT series in a thread.
    Returns candidate dict or None if failed.
    """
    try:
        series_started = time.perf_counter()
        s_num = s_data["SeriesNumber"]
        files = s_data["files"]
        
        if len(files) < 2: return None
        
        storage_stem = series_storage_stem(
            s_data["Modality"],
            s_num,
            s_data.get("SeriesDescriptionOriginal", ""),
            uid,
        )
        nii_filename = f"{storage_stem}.nii.gz"
        nii_path = case_output_dir / nii_filename
        
        # Convert
        convert_started = time.perf_counter()
        conversion = convert_series(
            s_num,
            files,
            nii_path,
            temp_dir,
            modality=s_data["Modality"],
        )
        if not conversion:
            return None
        convert_seconds = round(time.perf_counter() - convert_started, 3)
            
        # Phase Detection
        phase = "unknown"
        phase_seconds = 0.0
        phase_detected = False
        if s_data["Modality"] == "CT":
            json_path = case_output_dir / f"{storage_stem}.phase.json"
            phase_started = time.perf_counter()
            phase_data = run_totalseg_phase(nii_path, json_path)
            phase_seconds = round(time.perf_counter() - phase_started, 3)
            if phase_data:
                phase = phase_data.get("phase", "unknown")
                phase_detected = True
        
        # Return result dict
        return {
            "uid": uid,
            "series_number": s_num,
            "path": nii_path,
            "phase_json_path": json_path if s_data["Modality"] == "CT" else None,
            "num_slices": len(files),
            "kernel": s_data["ConvolutionKernel"].lower(),
            "kernel_raw": s_data["ConvolutionKernel"],
            "description": s_data["SeriesDescription"],
            "description_raw": s_data.get("SeriesDescriptionOriginal", ""),
            "modality": s_data["Modality"],
            "phase": phase,
            "convert_method": conversion["method"],
            "convert_seconds": convert_seconds,
            "phase_seconds": phase_seconds,
            "phase_detected": phase_detected,
            "series_total_seconds": round(time.perf_counter() - series_started, 3),
        }
    except Exception as e:
        print(f"  [Error] Failed to process series {s_data.get('SeriesNumber')}: {e}")
        return None

def run_totalseg_phase(input_nifti, output_json):
    """Runs totalseg_get_phase to detect CT contrast phase."""
    try:
        cmd = [
            TOTALSEG_GET_PHASE_BIN,
            "-i", str(input_nifti),
            "-o", str(output_json),
            "-q"
        ]
        result = subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        if output_json.exists():
            with open(output_json, 'r') as f:
                return json.load(f)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        if stderr:
            print(f"  Warning: Phase detection failed for {input_nifti.name}: {stderr}")
        else:
            print(f"  Warning: Phase detection failed for {input_nifti.name}: exit status {e.returncode}")
    except Exception as e:
        print(f"  Warning: Phase detection failed for {input_nifti.name}: {e}")
    return None

def is_4d_series(files_list):
    """
    Check if series is 4D (Time resolved) by checking for duplicate ImagePositions.
    """
    positions = set()
    for f in files_list:
        try:
            ds = pydicom.dcmread(str(f), stop_before_pixels=True)
            if hasattr(ds, "ImagePositionPatient"):
                pos = tuple(ds.ImagePositionPatient)
                if pos in positions:
                    return True # Duplicate position found -> 4D
                positions.add(pos)
        except:
            pass
    return False

def _detect_series_transfer_syntax_uid(files_list):
    """Return the transfer syntax UID used by the series, when readable."""
    for file_path in files_list:
        try:
            ds = pydicom.dcmread(str(file_path), stop_before_pixels=True, force=True)
        except Exception:
            continue
        file_meta = getattr(ds, "file_meta", None)
        transfer_syntax_uid = getattr(file_meta, "TransferSyntaxUID", None)
        if transfer_syntax_uid:
            return str(transfer_syntax_uid)
    return None


def _summarize_command_output(*chunks, max_lines=4):
    """Collapse stderr/stdout into a compact tail for operational logs."""
    lines = []
    for chunk in chunks:
        if not chunk:
            continue
        for line in str(chunk).splitlines():
            line = line.strip()
            if line:
                lines.append(line)
    if not lines:
        return None
    return " | ".join(lines[-max_lines:])


def _run_dcm2niix_conversion(dcm_in, dcm_out):
    """Execute dcm2niix and capture stderr for diagnostics."""
    return subprocess.run(
        [
            settings.DCM2NIIX_BIN,
            "-z",
            "y",
            "-f",
            "converted",
            "-o",
            str(dcm_out),
            str(dcm_in),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def _convert_series_with_dicom2nifti(dcm_in, output_nii_path):
    """Fallback converter for compressed CT series unsupported by dcm2niix."""
    try:
        dicom2nifti = importlib.import_module("dicom2nifti")
    except ImportError as exc:
        return f"dicom2nifti unavailable: {exc}"

    try:
        if output_nii_path.exists():
            output_nii_path.unlink()
        dicom2nifti.dicom_series_to_nifti(
            str(dcm_in),
            str(output_nii_path),
            reorient_nifti=False,
        )
    except Exception as exc:
        return str(exc)

    if not output_nii_path.exists():
        return "dicom2nifti completed without producing output"
    return None


def convert_series(series_id, files_list, output_nii_path, temp_dir, *, modality=None):
    """
    Converts a specific list of DICOM files to NIfTI.
    """
    dcm_in = temp_dir / f"dcm_{series_id}"
    dcm_in.mkdir(exist_ok=True)
    for f in files_list:
        shutil.copy(f, dcm_in)
        
    dcm_out = temp_dir / f"nii_{series_id}"
    dcm_out.mkdir(exist_ok=True)

    transfer_syntax_uid = _detect_series_transfer_syntax_uid(files_list)
    dcm2niix_result = _run_dcm2niix_conversion(dcm_in, dcm_out)
    generated = list(dcm_out.glob("*.nii.gz"))
    if generated:
        if dcm2niix_result.returncode != 0:
            summary = _summarize_command_output(dcm2niix_result.stderr)
            if summary:
                print(
                    f"    [Warning] dcm2niix returned exit status {dcm2niix_result.returncode} "
                    f"for series {series_id} but produced output: {summary}"
                )
        target_nii = max(generated, key=lambda p: p.stat().st_size)
        shutil.move(str(target_nii), str(output_nii_path))
        return {
            "method": "dcm2niix",
            "transfer_syntax_uid": transfer_syntax_uid,
        }

    summary = _summarize_command_output(dcm2niix_result.stderr)
    if dcm2niix_result.returncode != 0:
        if summary:
            print(f"    [Error] dcm2niix failed for series {series_id}: {summary}")
        else:
            print(
                f"    [Error] dcm2niix failed for series {series_id} "
                f"with exit status {dcm2niix_result.returncode}"
            )
    else:
        print(f"    [Error] dcm2niix produced no NIfTI output for series {series_id}")

    if modality == "CT" and transfer_syntax_uid in JPEG_LOSSLESS_TRANSFER_SYNTAXES:
        print(
            f"    [Prepare] Retrying series {series_id} with dicom2nifti "
            f"for transfer syntax {transfer_syntax_uid}"
        )
        fallback_error = _convert_series_with_dicom2nifti(dcm_in, output_nii_path)
        if not fallback_error:
            print(f"    [Prepare] dicom2nifti fallback succeeded for series {series_id}")
            return {
                "method": "dicom2nifti",
                "transfer_syntax_uid": transfer_syntax_uid,
            }
        print(f"    [Error] dicom2nifti fallback failed for series {series_id}: {fallback_error}")

    return None

def process_zip(zip_path):
    prepare_start_dt = datetime.datetime.now(LOCAL_TZ)
    prepare_start_time_str = prepare_start_dt.isoformat()
    stage_timings = {}
    prepare_stats = {}
    zip_path = Path(zip_path)
    if not zip_path.exists():
        raise FileNotFoundError(f"File {zip_path} not found.")
    upload_origin = _upload_origin_from_spool_path(zip_path)

    print(f"=== Prepare Upload: {unclaim_path(zip_path).name} ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir()
        min_series_images = settings.PREPARE_MIN_SERIES_IMAGES
        
        # 1. Extract ZIP
        extract_started = time.perf_counter()
        try:
            print("  Extracting...")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(extract_dir)
        except zipfile.BadZipFile:
             raise PrepareError("Invalid ZIP file.")
        stage_timings["extract_zip_seconds"] = round(time.perf_counter() - extract_started, 3)
        intake_manifest = {}
        intake_manifest_path = extract_dir / INTAKE_MANIFEST_NAME
        external_submission = load_external_submission_sidecar(zip_path)
        if intake_manifest_path.exists():
            try:
                with open(intake_manifest_path, "r", encoding="utf-8") as manifest_file:
                    intake_manifest = json.load(manifest_file)
            except Exception:
                intake_manifest = {}

        # 2. Scanning DICOMs
        scan_started = time.perf_counter()
        print("  Scanning DICOM files...")
        series_map = {}
        
        global_meta = {
            "PatientName": "Unknown",
            "PatientID": "",
            "PatientBirthDate": "",
            "PatientSex": "Unknown",
            "AccessionNumber": "000000",
            "StudyInstanceUID": "",
            "StudyDate": "", # Added
            "KVP": "Unknown", # Added
            "Modality": "", # Overall Modality match
            "Height": None,
            "Weight": None,
        }
        reference_dicom_context = {}
        
        found_ct = False
        
        for root, _, files in os.walk(extract_dir):
            for f in files:
                fpath = Path(root) / f
                try:
                    ds = pydicom.dcmread(str(fpath), stop_before_pixels=True)
                    if hasattr(ds, "SeriesInstanceUID"):
                        uid = ds.SeriesInstanceUID
                        modality = get_tag_value(ds, "Modality", "OT")
                        
                        if uid not in series_map:
                            series_map[uid] = {
                                "SeriesInstanceUID": uid,
                                "SeriesNumber": str(get_tag_value(ds, "SeriesNumber", "0")),
                                "Modality": modality,
                                "SliceThickness": float(get_tag_value(ds, "SliceThickness", 0.0) or 0.0),
                                "ConvolutionKernel": str(get_tag_value(ds, "ConvolutionKernel", "")),
                                "SeriesDescriptionOriginal": str(get_tag_value(ds, "SeriesDescription", "")).strip(),
                                "SeriesDescription": str(get_tag_value(ds, "SeriesDescription", "")).strip().lower(),
                                "files": []
                            }
                            # Global Metadata (first encounter)
                            if global_meta["PatientName"] == "Unknown":
                                name_val = get_tag_value(ds, "PatientName", "")
                                if name_val:
                                    global_meta["PatientName"] = normalize_patient_name_for_prepare(name_val)
                                global_meta["PatientID"] = str(get_tag_value(ds, "PatientID", "") or "")
                                global_meta["PatientBirthDate"] = str(get_tag_value(ds, "PatientBirthDate", "") or "")
                                global_meta["PatientSex"] = str(get_tag_value(ds, "PatientSex", "Unknown"))
                                global_meta["AccessionNumber"] = str(get_tag_value(ds, "AccessionNumber", "000000"))
                                global_meta["StudyInstanceUID"] = str(get_tag_value(ds, "StudyInstanceUID", ""))
                                global_meta["StudyDate"] = str(get_tag_value(ds, "StudyDate", ""))
                                global_meta["KVP"] = str(get_tag_value(ds, "KVP", "Unknown"))
                                global_meta["Modality"] = modality
                                global_meta["Height"] = parse_optional_float(get_tag_value(ds, "PatientSize", None))
                                global_meta["Weight"] = parse_optional_float(get_tag_value(ds, "PatientWeight", None))
                                reference_dicom_context = build_reference_dicom_context(ds)

                        series_map[uid]["files"].append(fpath)
                        if modality == "CT": found_ct = True
                        
                except Exception:
                    pass
        
        if not series_map:
            print("Error: No valid DICOM series found.")
            # sys.exit(1) # Don't exit, let's see what happens

        series_map, discarded_series = split_series_by_image_count(series_map, min_series_images)
        stage_timings["scan_dicoms_seconds"] = round(time.perf_counter() - scan_started, 3)
        prepare_stats["min_series_images"] = min_series_images
        prepare_stats["total_series_detected"] = len(series_map) + len(discarded_series)
        prepare_stats["series_discarded_before_conversion"] = len(discarded_series)
        prepare_stats["series_kept_for_conversion"] = len(series_map)
        prepare_stats["ct_series_detected"] = sum(1 for s in series_map.values() if s["Modality"] == "CT")
        prepare_stats["mr_series_detected"] = sum(1 for s in series_map.values() if s["Modality"] == "MR")
        if discarded_series:
            print(f"  Discarded {len(discarded_series)} series with fewer than {min_series_images} images")

        if not series_map:
            raise PrepareError(f"No series met the minimum image threshold ({min_series_images}).")

        # Determine Global Modality
        if found_ct: 
            global_meta["Modality"] = "CT"
        
        exam_modality = global_meta["Modality"]
        print(f"  Exam Modality Detected: {exam_modality}")

        # ... (Output Dir Setup skipped in this chunk) ...

        # 3. Setup Output Dir
        # --- NEW: Get Date/Accession for Clinical Name/CaseID ---
        study_date = global_meta.get("StudyDate", "00000000")
        acc_num = global_meta.get("AccessionNumber", "000000")
        
        clinical_name = generate_clinical_name(global_meta["PatientName"], str(study_date), str(acc_num))
        print(f"  Clinical Name (CaseID): {clinical_name}")
        print(
            f"[Prepare] Case {clinical_name}: modality={exam_modality}, "
            f"series={prepare_stats['series_kept_for_conversion']}"
        )

        # Override CaseID with ClinicalName
        case_id = clinical_name
        study_uid = str(global_meta.get("StudyInstanceUID", "") or "").strip()
        manifest_study_uid, manifest_digest = _extract_manifest_fingerprint(intake_manifest)
        if not manifest_study_uid:
            manifest_study_uid = study_uid
        if not manifest_digest and manifest_study_uid:
            manifest_digest = build_study_manifest_digest(
                extract_dir,
                study_uid=manifest_study_uid,
                calling_aet=str(intake_manifest.get("calling_aet", "") or "").strip() or None,
                instance_count=_safe_int(intake_manifest.get("instance_count"), 0),
                ignored_names={INTAKE_MANIFEST_NAME},
            )
            intake_manifest["manifest_digest"] = manifest_digest
        if manifest_study_uid and manifest_digest:
            if _should_skip_duplicate_prepare_manifest(
                study_uid=manifest_study_uid,
                manifest_digest=manifest_digest,
                case_id=case_id,
            ):
                print(
                    f"[Prepare] Duplicate manifest suppressed for {case_id}; "
                    "existing prepared study already available"
                )
                _delete_spooled_zip(zip_path)
                return
            conn = db_connect()
            try:
                store.update_study_handoff_state(
                    conn,
                    study_uid=manifest_study_uid,
                    manifest_digest=manifest_digest,
                    case_id=case_id,
                    status="preparing",
                    last_error=None,
                )
            finally:
                conn.close()

        case_output_dir = study_artifacts_dir(case_id)
        case_output_dir.mkdir(parents=True, exist_ok=True)
        study_metadata_dir(case_id).mkdir(parents=True, exist_ok=True)
        derived_dir = study_derived_dir(case_id)
        derived_dir.mkdir(parents=True, exist_ok=True)
        derived_series_dir = derived_dir / "series"
        if derived_series_dir.exists():
            shutil.rmtree(derived_series_dir)
        derived_series_dir.mkdir(parents=True, exist_ok=True)
        
        id_data = {
            "PatientName": global_meta["PatientName"],
            "PatientID": global_meta["PatientID"],
            "PatientBirthDate": global_meta["PatientBirthDate"],
            "PatientSex": global_meta["PatientSex"],
            "AccessionNumber": global_meta["AccessionNumber"],
            "StudyInstanceUID": global_meta["StudyInstanceUID"],
            "Modality": exam_modality,
            "StudyDate": str(study_date),
            "CaseID": case_id,
            "ClinicalName": clinical_name,
            "KVP": global_meta.get("KVP", "Unknown")
        }
        metadata_data = {
            "PatientName": global_meta["PatientName"],
            "PatientID": global_meta["PatientID"],
            "PatientBirthDate": global_meta["PatientBirthDate"],
            "PatientSex": global_meta["PatientSex"],
            "StudyInstanceUID": global_meta["StudyInstanceUID"],
            "AccessionNumber": global_meta["AccessionNumber"],
            "StudyDate": str(study_date),
            "CaseID": case_id,
            "ClinicalName": clinical_name,
        }
        if global_meta.get("Height") is not None:
            metadata_data["Height"] = global_meta["Height"]
        if global_meta.get("Weight") is not None:
            metadata_data["Weight"] = global_meta["Weight"]
        if reference_dicom_context:
            metadata_data["ReferenceDicom"] = reference_dicom_context

        # Insert into DB immediately
        init_and_insert_db(id_data)
        if global_meta.get("StudyInstanceUID"):
            update_db_biometrics(
                global_meta["StudyInstanceUID"],
                weight=metadata_data.get("Weight"),
                height=metadata_data.get("Height"),
            )
        
        with open(study_id_json(case_id), "w") as f:
            json.dump(id_data, f, indent=2)
        with open(study_metadata_json(case_id), "w") as f:
            json.dump(metadata_data, f, indent=2)

        available_series = []
        phase_detection_available = Path(TOTALSEG_GET_PHASE_BIN).exists() or shutil.which(TOTALSEG_GET_PHASE_BIN) is not None
        prepare_stats["phase_detection_available"] = phase_detection_available
        
        if exam_modality == "MR":
            # --- MR LOGIC: CONVERT AND PRESERVE ALL SERIES ---
            select_started = time.perf_counter()
            print("  [MR Mode] Converting and preserving all MR series...")
            mr_candidates = []
            
            for uid, s_data in series_map.items():
                # Filter out obvious non-MR or small stuff if deemed necessary
                # if s_data["Modality"] != "MR": continue 

                # 4D Check
                # Just assuming 'files' list order is somewhat valid, or random access is fine
                is_4d = is_4d_series(s_data["files"])
                
                score = len(s_data["files"])
                if is_4d: score = -5000 # Penalize heavily but don't crash
                if len(s_data["files"]) < 2: score = -9000
                
                mr_candidates.append({
                    "uid": uid,
                    "s_data": s_data,
                    "score": score,
                    "is_4d": is_4d
                })
            
            # Sort best first
            mr_candidates.sort(key=lambda x: x["score"], reverse=True)
            
            if not mr_candidates:
                raise PrepareError("No valid MR candidates.")
                
            stage_timings["select_and_convert_seconds"] = round(time.perf_counter() - select_started, 3)
            prepare_stats["selection_mode"] = "deferred_to_segmentation"
            prepare_stats["candidate_series_considered"] = len(mr_candidates)
            converted_candidates = []
            for candidate in mr_candidates:
                s_data = candidate["s_data"]
                s_num = s_data["SeriesNumber"]
                storage_stem = series_storage_stem(
                    s_data["Modality"],
                    s_num,
                    s_data.get("SeriesDescriptionOriginal", ""),
                    candidate["uid"],
                )
                nii_path = derived_series_dir / f"{storage_stem}.nii.gz"

                print(f"    Converting Series {s_num}...")
                conversion = convert_series(
                    s_num,
                    s_data["files"],
                    nii_path,
                    temp_dir,
                    modality=s_data["Modality"],
                )
                if not conversion:
                    continue

                series_info = {
                    "SeriesInstanceUID": candidate["uid"],
                    "SeriesNumber": s_num,
                    "Modality": s_data["Modality"],
                    "SeriesDescription": s_data.get("SeriesDescriptionOriginal", ""),
                    "ConvolutionKernel": s_data["ConvolutionKernel"],
                    "SliceCount": len(s_data["files"]),
                    "Is4D": candidate["is_4d"],
                    "SelectionScore": candidate["score"],
                    "ConvertMethod": conversion["method"],
                    "DerivedNiftiPath": str(nii_path.relative_to(derived_dir)),
                }
                available_series.append(series_info)
                candidate["path"] = nii_path
                converted_candidates.append(candidate)

            if not converted_candidates:
                raise PrepareError("Conversion of MR series failed.")

            prepare_stats["converted_series"] = len(converted_candidates)
            print(f"  Preserved MR series: {len(converted_candidates)}")

        else:
            # --- CT LOGIC: CONVERT, PRESERVE AND CHARACTERIZE ALL SERIES ---
            select_started = time.perf_counter()
            print("  [CT Mode] Converting, preserving and characterizing all CT series (Parallel - 5 Threads)...")
            candidates = []
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for uid, s_data in series_map.items():
                    futures.append(
                        executor.submit(process_ct_series_concurrency, uid, s_data, derived_series_dir, temp_dir)
                    )
                
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        candidates.append(result)
                        print(f"    Series {result['series_number']}: {result['num_slices']} slices, Phase: {result['phase']}, Kernel: {result['kernel']}")
            
            if not candidates:
                 raise PrepareError("No valid CT series converted.")
            stage_timings["select_and_convert_seconds"] = round(time.perf_counter() - select_started, 3)
            prepare_stats["selection_mode"] = "deferred_to_segmentation"
            prepare_stats["candidate_series_considered"] = len(candidates)
            prepare_stats["phase_detection_runs"] = sum(1 for c in candidates if c.get("phase_seconds", 0.0) > 0)
            prepare_stats["phase_detection_successes"] = sum(1 for c in candidates if c.get("phase_detected"))
            stage_timings["convert_series_total_seconds"] = round(sum(float(c.get("convert_seconds", 0.0) or 0.0) for c in candidates), 3)
            stage_timings["phase_detection_total_seconds"] = round(sum(float(c.get("phase_seconds", 0.0) or 0.0) for c in candidates), 3)
            stage_timings["candidate_series_total_seconds"] = round(sum(float(c.get("series_total_seconds", 0.0) or 0.0) for c in candidates), 3)
            prepare_stats["converted_series"] = len(candidates)
            prepare_stats["fallback_converted_series"] = sum(
                1 for c in candidates if c.get("convert_method") == "dicom2nifti"
            )
            for candidate in candidates:
                phase_data = {}
                phase_json_path = candidate.get("phase_json_path")
                if phase_json_path and phase_json_path.exists():
                    try:
                        with open(phase_json_path, "r") as phase_file:
                            phase_data = json.load(phase_file)
                    except Exception:
                        phase_data = {}
                available_series.append({
                    "SeriesInstanceUID": candidate["uid"],
                    "SeriesNumber": candidate["series_number"],
                    "Modality": candidate["modality"],
                    "SeriesDescription": candidate.get("description_raw", ""),
                    "ConvolutionKernel": candidate.get("kernel_raw", ""),
                    "SliceCount": candidate["num_slices"],
                    "DetectedPhase": candidate["phase"],
                    "PhaseDetected": candidate["phase_detected"],
                    "PhaseDetectionSeconds": candidate.get("phase_seconds", 0.0),
                    "PhaseData": phase_data,
                    "DerivedNiftiPath": str(candidate["path"].relative_to(derived_dir)),
                    "PhaseJsonPath": str(phase_json_path.relative_to(derived_dir)) if phase_json_path and phase_json_path.exists() else None,
                })
            print(f"  Preserved CT series: {len(candidates)}")
            
            # --- Update full metadata from a representative CT instance ---
            try:
                w_uid = candidates[0]['uid']
                w_files = series_map[w_uid]['files']
                if w_files:
                    first_dcm = w_files[0]
                    ds_full = pydicom.dcmread(str(first_dcm), stop_before_pixels=True)
                    full_meta = extract_full_dicom_metadata(ds_full)
                    update_db_full_metadata(global_meta["StudyInstanceUID"], full_meta)
                    reference_dicom_context = build_reference_dicom_context(ds_full)
            except Exception as e:
                print(f"  [Error] Failed to extract/update full metadata: {e}")

        # 5. Final Handover & Metadata Enrichment
        if available_series:
            duplicate_skip_context = _completed_case_skip_context(
                case_id,
                {
                    **id_data,
                    "AvailableSeries": available_series,
                    "DiscardedSeries": discarded_series,
                },
            )
            prepare_end_dt = datetime.datetime.now(LOCAL_TZ)
            prepare_elapsed_str = str(prepare_end_dt - prepare_start_dt)
            stage_timings["total_prepare_seconds"] = round((prepare_end_dt - prepare_start_dt).total_seconds(), 3)

            prepare_pipeline_updates = {
                "prepare_start_time": prepare_start_time_str,
                "prepare_end_time": prepare_end_dt.isoformat(),
                "prepare_elapsed_time": prepare_elapsed_str,
                "prepare_stage_timings_seconds": stage_timings,
                "prepare_stats": prepare_stats,
                "prepare_input_origin": upload_origin,
            }
            if external_submission:
                prepare_pipeline_updates["external_job_id"] = external_submission.get("job_id")
                prepare_pipeline_updates["external_client_case_id"] = external_submission.get("client_case_id")
                prepare_pipeline_updates["external_submission_received_at"] = external_submission.get("received_at")
            if intake_manifest:
                prepare_pipeline_updates["intake_first_instance_time"] = intake_manifest.get("first_instance_time")
                prepare_pipeline_updates["intake_last_instance_time"] = intake_manifest.get("last_instance_time")
                prepare_pipeline_updates["intake_receive_elapsed_time"] = intake_manifest.get("receive_elapsed_time")
                prepare_pipeline_updates["intake_receive_elapsed_seconds"] = intake_manifest.get("receive_elapsed_seconds")
                prepare_pipeline_updates["intake_instance_count"] = intake_manifest.get("instance_count")
                prepare_pipeline_updates["intake_calling_aet"] = intake_manifest.get("calling_aet")
                prepare_pipeline_updates["intake_remote_ip"] = intake_manifest.get("remote_ip")
                prepare_pipeline_updates["intake_handoff_time"] = intake_manifest.get("handoff_time")
                handoff_time = intake_manifest.get("handoff_time")
                if handoff_time:
                    try:
                        handoff_dt = datetime.datetime.fromisoformat(handoff_time)
                        prepare_pipeline_updates["handoff_to_prepare_elapsed_time"] = str(
                            prepare_start_dt - handoff_dt
                        )
                    except Exception:
                        prepare_pipeline_updates["handoff_to_prepare_elapsed_time"] = (
                            "Error parsing intake_handoff_time"
                        )

            output_meta, output_metadata = _build_prepare_output_payloads(
                case_id=case_id,
                id_data=id_data,
                metadata_data=metadata_data,
                available_series=available_series,
                discarded_series=discarded_series,
                prepare_pipeline_updates=prepare_pipeline_updates,
                duplicate_skip_context=duplicate_skip_context,
                reference_dicom_context=reference_dicom_context,
            )
            if external_submission:
                external_delivery_payload = {
                    "job_id": str(external_submission.get("job_id", "") or "").strip() or None,
                    "client_case_id": str(external_submission.get("client_case_id", "") or "").strip() or None,
                    "callback_url": str(external_submission.get("callback_url", "") or "").strip() or None,
                    "source_system": str(external_submission.get("source_system", "") or "").strip() or None,
                    "requested_outputs": external_submission.get("requested_outputs", {}),
                    "requested_metrics_modules": external_submission.get("requested_metrics_modules", []),
                    "received_at": external_submission.get("received_at"),
                }
                output_meta["ExternalDelivery"] = external_delivery_payload
                output_metadata["ExternalDelivery"] = external_delivery_payload
            
            # Save updated id.json
            with open(study_id_json(case_id), "w") as f:
                json.dump(output_meta, f, indent=2)
            with open(study_metadata_json(case_id), "w") as f:
                json.dump(output_metadata, f, indent=2)

            try:
                study_uid = output_meta.get("StudyInstanceUID")
                if study_uid:
                    conn = db_connect()
                    store.update_id_json(conn, study_uid, output_meta)
                    conn.close()
                    print(f"  [DB] id.json updated for {study_uid}")
                    update_db_biometrics(
                        study_uid,
                        weight=output_metadata.get("Weight"),
                        height=output_metadata.get("Height"),
                    )
            except Exception as e:
                print(f"  [Warning] Failed to update prepare timing in DB: {e}")

            if duplicate_skip_context:
                print(
                    f"[Prepare] Reusing completed pipeline for {case_id}; "
                    "skipping segmentation enqueue"
                )
            else:
                enqueue_case_for_segmentation(case_id)
                print(f"[Prepare] Enqueued for segmentation: {case_id}")
                print(f"\n  Ready: {study_dir(case_id)}")
                print(str(study_dir(case_id)))

            if manifest_study_uid and manifest_digest:
                conn = db_connect()
                try:
                    store.update_study_handoff_state(
                        conn,
                        study_uid=manifest_study_uid,
                        manifest_digest=manifest_digest,
                        case_id=case_id,
                        status="prepared",
                        last_error=None,
                    )
                finally:
                    conn.close()

            if not duplicate_skip_context:
                try:
                    enqueued_dispatches = enqueue_patient_identified_dispatches(
                        id_data=output_meta,
                        metadata_data=output_metadata,
                        intake_manifest=intake_manifest,
                    )
                    if enqueued_dispatches:
                        print(
                            f"[Prepare] Enqueued {enqueued_dispatches} patient-identified "
                            f"integration event(s) for {case_id}"
                        )
                except Exception as e:
                    print(f"  [Warning] Failed to enqueue integration events for {case_id}: {e}")

            print(f"[Prepare] ✓ Complete {case_id} ({prepare_elapsed_str})")

        else:
             raise PrepareError("No series were successfully converted.")

    # Clean up input ZIP
    _delete_spooled_zip(zip_path)


def process_spooled_zip(zip_path: Path) -> bool:
    """Process a claimed spool ZIP and keep the watchdog alive on failures."""
    intake_manifest = _load_intake_manifest_from_zip(zip_path)
    manifest_study_uid, manifest_digest = _extract_manifest_fingerprint(intake_manifest)
    try:
        print(f"[Prepare] Claimed upload: {unclaim_path(zip_path).name}")
        process_zip(zip_path)
        return True
    except Exception as exc:
        if manifest_study_uid and manifest_digest:
            conn = db_connect()
            try:
                store.update_study_handoff_state(
                    conn,
                    study_uid=manifest_study_uid,
                    manifest_digest=manifest_digest,
                    status="error",
                    last_error=str(exc),
                )
            finally:
                conn.close()
        failed_path = move_failed_upload(zip_path) if zip_path.exists() else None
        print(f"✗ Prepare failed for {unclaim_path(zip_path).name}: {exc}")
        if failed_path is not None:
            print(f"  Failed ZIP moved to: {failed_path}")
        return False


def _spool_sort_key(path: Path) -> tuple[float, str]:
    try:
        stat = path.stat()
        return (stat.st_mtime, path.name)
    except FileNotFoundError:
        return (float("inf"), path.name)


def _upload_origin_from_spool_path(zip_path: Path) -> str:
    base_path = unclaim_path(zip_path)
    from_prepare_dir = getattr(settings, "UPLOAD_FROM_PREPARE_DIR", None)
    external_dir = getattr(settings, "UPLOAD_EXTERNAL_DIR", None)
    if from_prepare_dir is not None and base_path.parent == from_prepare_dir:
        return "P"
    if external_dir is not None and base_path.parent == external_dir:
        return "E"
    return ""


def _iter_claimable_uploads_in_dir(spool_dir: Path):
    for path in sorted(spool_dir.glob(f"*.zip{CLAIM_SUFFIX}"), key=_spool_sort_key):
        yield path
    for path in sorted(spool_dir.glob("*.zip"), key=_spool_sort_key):
        if is_spooled_zip_stable(path):
            try:
                yield claim_path(path)
            except FileNotFoundError:
                continue


def iter_claimable_uploads():
    """Yield FIFO uploads, prioritizing from_prepare over external spool sources."""
    seen = set()
    for spool_dir in (
        getattr(settings, "UPLOAD_FROM_PREPARE_DIR", None),
        getattr(settings, "UPLOAD_EXTERNAL_DIR", None),
        settings.UPLOAD_DIR,
    ):
        if spool_dir is None:
            continue
        spool_key = str(spool_dir)
        if spool_key in seen:
            continue
        seen.add(spool_key)
        yield from _iter_claimable_uploads_in_dir(spool_dir)


def watch_upload_spool() -> int:
    """Run the prepare watchdog loop over the upload spool."""
    from_prepare_dir = getattr(settings, "UPLOAD_FROM_PREPARE_DIR", settings.UPLOAD_DIR)
    external_dir = getattr(settings, "UPLOAD_EXTERNAL_DIR", settings.UPLOAD_DIR)
    print("Starting prepare watchdog...")
    print(f"  Upload spool (from_prepare): {from_prepare_dir}")
    print(f"  Upload spool (external): {external_dir}")
    print(f"  Failed spool: {settings.UPLOAD_FAILED_DIR}")
    print(f"  Stable age: {settings.PREPARE_STABLE_AGE_SECONDS}s")
    print(f"  Scan interval: {settings.PREPARE_SCAN_INTERVAL}s")

    try:
        while True:
            handled_any = False
            for zip_path in iter_claimable_uploads():
                handled_any = True
                process_spooled_zip(zip_path)
            if not handled_any:
                time.sleep(settings.PREPARE_SCAN_INTERVAL)
    except KeyboardInterrupt:
        print("\nStopping prepare watchdog...")
        return 0

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("zip_path", nargs="?", help="Path to DICOM ZIP")
    args = parser.parse_args(argv)
    if args.zip_path:
        try:
            process_zip(args.zip_path)
        except Exception as exc:
            print(f"Prepare failed: {exc}")
            return 1
        return 0
    return watch_upload_spool()


if __name__ == "__main__":
    raise SystemExit(main())
