#!/usr/bin/env python3
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

"""
Heimdallr DICOM Listener (dicom_listener.py)

DICOM C-STORE SCP (Service Class Provider) that:
1. Receives DICOM images from modalities/PACS via DICOM protocol
2. Groups images by StudyInstanceUID
3. Automatically closes studies after idle timeout (no new images)
4. Zips completed studies and uploads to Heimdallr server
5. Handles upload retries with exponential backoff

Usage:
    python -m heimdallr.intake

All configuration is centralized in `heimdallr.shared.settings` and can be overridden via environment variables.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import os
import shutil
import signal
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import requests
from pydicom.dataset import Dataset
from pynetdicom import AE, ALL_TRANSFER_SYNTAXES, AllStoragePresentationContexts, _config, evt
from pynetdicom.sop_class import Verification

from heimdallr.shared import settings, store
from heimdallr.shared.spool import atomic_write_bytes
from heimdallr.shared.sqlite import connect as db_connect

LOCAL_TZ = ZoneInfo(settings.TIMEZONE)
INTAKE_MANIFEST_NAME = "_heimdallr_intake.json"


@dataclass
class StudyState:
    """
    Tracks the state of an active DICOM study being received.
    
    Attributes:
        study_uid: StudyInstanceUID (sanitized for filesystem)
        path: Directory where DICOM files are stored
        first_update_ts: Timestamp of first received image
        last_update_ts: Timestamp of last received image
        instance_count: Number of received DICOM instances
        locked: Whether study is currently being processed/uploaded
    """
    study_uid: str
    path: Path
    first_update_ts: float
    last_update_ts: float
    calling_aet: str | None = None
    remote_ip: str | None = None
    instance_count: int = 0
    locked: bool = False


def safe_mkdir(p: Path) -> None:
    """Create directory and all parent directories if they don't exist."""
    p.mkdir(parents=True, exist_ok=True)


def now() -> float:
    """Get current Unix timestamp."""
    return time.time()


def isoformat_local(ts: float) -> str:
    """Render a Unix timestamp in the configured local timezone."""
    return datetime.datetime.fromtimestamp(ts, tz=LOCAL_TZ).isoformat()


def sanitize_filename(s: str) -> str:
    """
    Sanitize string for use as filename/directory name.
    
    Removes special characters and limits length to prevent filesystem issues.
    """
    return "".join(c for c in s if c.isalnum() or c in ("-", "_", "."))[:200] or "unknown"


def normalize_optional_text(value: object) -> str | None:
    """Return a stripped string value or None."""
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def extract_requestor_identity(event) -> tuple[str | None, str | None]:
    """Extract the upstream Calling AE Title and remote IP from a pynetdicom event."""
    assoc = getattr(event, "assoc", None)
    requestor = getattr(assoc, "requestor", None)
    primitive = getattr(requestor, "primitive", None)

    calling_aet = normalize_optional_text(getattr(requestor, "ae_title", None))
    if not calling_aet:
        calling_aet = normalize_optional_text(getattr(primitive, "calling_ae_title", None))

    remote_ip = None
    try:
        remote_ip = normalize_optional_text(getattr(requestor, "address", None))
    except Exception:
        pass

    if not remote_ip:
        address_info = getattr(requestor, "address_info", None)
        remote_ip = normalize_optional_text(getattr(address_info, "address", None))

    if not remote_ip:
        presentation_address = getattr(primitive, "calling_presentation_address", None)
        remote_ip = normalize_optional_text(getattr(presentation_address, "address", None))

    return calling_aet, remote_ip


def write_instance(ds: Dataset, out_dir: Path) -> Path:
    """
    Write a DICOM instance to disk in organized directory structure.
    
    Structure: {study_dir}/{series_uid}/{sop_uid}.dcm
    
    Args:
        ds: DICOM dataset to save
        out_dir: Study directory (will create series subdirectory)
    
    Returns:
        Path to saved DICOM file
    """
    study_uid = str(getattr(ds, "StudyInstanceUID", ""))
    series_uid = str(getattr(ds, "SeriesInstanceUID", ""))
    sop_uid = str(getattr(ds, "SOPInstanceUID", ""))

    # Sanitize UIDs for filesystem
    series_uid_s = sanitize_filename(series_uid) if series_uid else "series_unknown"
    sop_uid_s = sanitize_filename(sop_uid) if sop_uid else f"inst_{int(now()*1000)}"

    # Create series subdirectory
    series_dir = out_dir / series_uid_s
    safe_mkdir(series_dir)

    # Atomic write: write to temp file then rename
    out_path = series_dir / f"{sop_uid_s}.dcm"
    tmp_path = out_path.with_suffix(".dcm.tmp")
    ds.save_as(tmp_path, write_like_original=False)
    tmp_path.replace(out_path)
    
    return out_path


def build_intake_manifest(study: StudyState, *, handoff_ts: float) -> dict:
    """Build a transport manifest for prepare to persist intake timings."""
    receive_elapsed_seconds = max(0.0, study.last_update_ts - study.first_update_ts)
    return {
        "study_uid": study.study_uid,
        "first_instance_time": isoformat_local(study.first_update_ts),
        "last_instance_time": isoformat_local(study.last_update_ts),
        "receive_elapsed_seconds": round(receive_elapsed_seconds, 3),
        "receive_elapsed_time": str(datetime.timedelta(seconds=round(receive_elapsed_seconds, 6))),
        "instance_count": study.instance_count,
        "calling_aet": study.calling_aet,
        "remote_ip": study.remote_ip,
        "handoff_time": isoformat_local(handoff_ts),
    }


def zip_study(study_dir: Path, *, intake_manifest: dict | None = None) -> bytes:
    """
    Create ZIP archive of entire study directory.
    
    Args:
        study_dir: Directory containing DICOM files (organized by series)
    
    Returns:
        ZIP file contents as bytes
    """
    buff = io.BytesIO()
    with zipfile.ZipFile(buff, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(study_dir):
            for fn in files:
                # Skip macOS metadata files
                if fn == ".DS_Store":
                    continue
                fpath = Path(root) / fn
                # Preserve directory structure within ZIP
                rel = fpath.relative_to(study_dir)
                zf.write(fpath, arcname=str(rel))
        if intake_manifest:
            zf.writestr(INTAKE_MANIFEST_NAME, json.dumps(intake_manifest, indent=2))
    buff.seek(0)
    return buff.read()


def upload_zip(zip_bytes: bytes, upload_url: str, token: Optional[str], timeout: int) -> requests.Response:
    """
    Upload ZIP file to Heimdallr server.
    
    Args:
        zip_bytes: ZIP file contents
        upload_url: Server upload endpoint
        token: Optional bearer token for authentication
        timeout: Request timeout in seconds
    
    Returns:
        HTTP response from server
    """
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    files = {
        "file": ("study.zip", zip_bytes, "application/zip")
    }
    return requests.post(upload_url, headers=headers, files=files, timeout=timeout)


class HeimdallrDicomListener:
    """
    DICOM C-STORE SCP with automatic study completion and upload.
    
    Workflow:
    1. Receives DICOM images via C-STORE
    2. Groups by StudyInstanceUID
    3. Monitors for idle timeout (no new images)
    4. Zips and uploads completed studies
    5. Cleans up on success, archives on failure
    """
    
    def __init__(
        self,
        incoming_dir: Path,
        failed_dir: Path,
        state_dir: Path,
        idle_seconds: int,
        upload_url: str,
        upload_token: Optional[str],
        upload_timeout: int,
        upload_retries: int,
        upload_backoff: int,
        handoff_mode: str,
        upload_staging_dir: Path,
    ) -> None:
        """
        Initialize DICOM listener.
        
        Args:
            incoming_dir: Directory for receiving DICOM files
            failed_dir: Archive for failed uploads
            state_dir: Directory for persistent state (future use)
            idle_seconds: Time without new images to close study
            upload_url: Heimdallr server upload endpoint
            upload_token: Optional authentication token
            upload_timeout: HTTP request timeout
            upload_retries: Number of upload attempts
            upload_backoff: Seconds between retry attempts
            handoff_mode: Delivery mode for completed studies ("local_prepare" or "http_upload")
            upload_staging_dir: Directory where ZIPs are staged for local_prepare handoff
        """
        self.incoming_dir = incoming_dir
        self.failed_dir = failed_dir
        self.state_dir = state_dir

        self.idle_seconds = idle_seconds

        self.upload_url = upload_url
        self.upload_token = upload_token
        self.upload_timeout = upload_timeout
        self.upload_retries = upload_retries
        self.upload_backoff = upload_backoff
        self.handoff_mode = handoff_mode
        self.upload_staging_dir = upload_staging_dir

        # Active studies being received
        self.studies: Dict[str, StudyState] = {}
        self._stop = False

        # Ensure all directories exist
        for p in (incoming_dir, failed_dir, state_dir, upload_staging_dir):
            safe_mkdir(p)

    def stop(self) -> None:
        """Signal listener to stop (called by signal handlers)."""
        self._stop = True

    def on_c_store(self, event) -> int:
        """
        Handle incoming DICOM C-STORE request.
        
        Called by pynetdicom for each received DICOM instance.
        
        Args:
            event: C-STORE event containing DICOM dataset
        
        Returns:
            DICOM status code (0x0000 = success, 0xA700 = failure)
        """
        try:
            ds = event.dataset
            ds.file_meta = event.file_meta

            # Extract StudyInstanceUID
            study_uid = str(getattr(ds, "StudyInstanceUID", "")).strip()
            if not study_uid:
                # Fallback for malformed DICOM without StudyInstanceUID
                study_uid = f"study_unknown_{int(now()*1000)}"

            calling_aet, remote_ip = extract_requestor_identity(event)

            study_uid_s = sanitize_filename(study_uid)
            study_dir = self.incoming_dir / study_uid_s
            safe_mkdir(study_dir)

            # Write DICOM file to disk
            write_instance(ds, study_dir)

            # Update study state
            st = self.studies.get(study_uid_s)
            ts = now()
            if not st:
                # New study
                st = StudyState(
                    study_uid=study_uid_s,
                    path=study_dir,
                    first_update_ts=ts,
                    last_update_ts=ts,
                    calling_aet=calling_aet,
                    remote_ip=remote_ip,
                    instance_count=1,
                )
                self.studies[study_uid_s] = st
                persist_intake_metadata = True
            else:
                # Existing study: update timestamp
                st.last_update_ts = ts
                st.instance_count += 1
                persist_intake_metadata = False
                if calling_aet and calling_aet != st.calling_aet:
                    st.calling_aet = calling_aet
                    persist_intake_metadata = True
                if remote_ip and remote_ip != st.remote_ip:
                    st.remote_ip = remote_ip
                    persist_intake_metadata = True

            if persist_intake_metadata and (st.calling_aet or st.remote_ip):
                conn = None
                try:
                    conn = db_connect()
                    store.upsert_intake_metadata(
                        conn,
                        study_uid,
                        calling_aet=st.calling_aet,
                        remote_ip=st.remote_ip,
                    )
                except Exception as db_exc:
                    print(f"⚠ Failed to persist intake metadata for {study_uid}: {db_exc}")
                finally:
                    if conn is not None:
                        conn.close()

            # Return success status
            return 0x0000
            
        except Exception as e:
            try:
                sop_class_uid = str(getattr(getattr(event, "file_meta", None), "MediaStorageSOPClassUID", "unknown"))
            except Exception:
                sop_class_uid = "unknown"
            print(f"✗ C-STORE handling error (SOP Class: {sop_class_uid}): {e}")
            # Unexpected error: return segmentation failure status
            return 0xA700

    def scan_and_flush(self) -> None:
        """
        Scan active studies and upload those that have been idle.
        
        Called periodically by main loop to check for completed studies.
        Studies are considered complete if no new images received for idle_seconds.
        """
        cutoff = now() - self.idle_seconds
        
        for study_uid, st in list(self.studies.items()):
            # Skip locked studies (already being processed)
            if st.locked:
                continue
                
            # Skip studies that are still receiving images
            if st.last_update_ts > cutoff:
                continue

            # Study is idle: process and upload
            st.locked = True
            try:
                handoff_ts = now()
                intake_manifest = build_intake_manifest(st, handoff_ts=handoff_ts)
                # Create ZIP archive
                zip_bytes = zip_study(st.path, intake_manifest=intake_manifest)

                # Attempt handoff with retries
                ok = False
                last_exc: Optional[Exception] = None
                
                for attempt in range(1, self.upload_retries + 1):
                    try:
                        if self.handoff_mode == "local_prepare":
                            staged_name = f"study_{settings.local_timestamp('%Y%m%d%H%M%S')}_{study_uid}.zip"
                            staged_zip = self.upload_staging_dir / staged_name
                            atomic_write_bytes(staged_zip, zip_bytes)
                            ok = True
                            break
                        else:
                            resp = upload_zip(
                                zip_bytes,
                                self.upload_url,
                                self.upload_token,
                                self.upload_timeout
                            )
                            if 200 <= resp.status_code < 300:
                                ok = True
                                break
                            # Server returned error: wait and retry
                            time.sleep(self.upload_backoff)
                            
                    except Exception as e:
                        last_exc = e
                        time.sleep(self.upload_backoff)

                # Generate timestamped filename for archive
                ts = settings.local_timestamp("%Y%m%d%H%M%S")
                zip_name = f"{ts}_{study_uid}.zip"
                
                if ok:
                    # Successful handoff: raw DICOM staging can be removed.
                    # ZIP payloads are disposable in the current storage model.
                    shutil.rmtree(st.path, ignore_errors=True)
                    
                    # Remove from active studies
                    self.studies.pop(study_uid, None)
                    
                    if self.handoff_mode == "local_prepare":
                        print(f"✓ Study {study_uid} queued for prepare watchdog")
                    else:
                        print(f"✓ Study {study_uid} uploaded successfully")
                    
                else:
                    # Upload failed: save a disposable ZIP snapshot for review
                    fail_path = self.failed_dir / zip_name
                    fail_path.write_bytes(zip_bytes)
                    
                    # Remove from active studies (keep raw DICOM for investigation)
                    self.studies.pop(study_uid, None)
                    
                    print(f"✗ Study {study_uid} handoff failed after {self.upload_retries} attempts")
                    if last_exc:
                        print(f"  Last error: {last_exc}")
                        
            finally:
                st.locked = False


def build_ae(ae_title: str) -> AE:
    """
    Build a permissive DICOM Application Entity for storage intake.
    
    The listener should behave as an operational intake gateway and accept all
    storage objects offered by upstream modalities/PACS, including non-image
    payloads such as encapsulated documents and structured reports.
    
    Args:
        ae_title: DICOM AE title for this listener
    
    Returns:
        Configured Application Entity
    """
    # Treat unknown/private storage abstract syntaxes as valid storage classes.
    _config.UNRESTRICTED_STORAGE_SERVICE = True

    ae = AE(ae_title=ae_title)
    ae.add_supported_context(Verification)

    # Advertise every known Storage SOP class with all transfer syntaxes,
    # including compressed encodings, to avoid needless association rejection.
    storage_sop_classes = [context.abstract_syntax for context in AllStoragePresentationContexts]
    for abstract_syntax in storage_sop_classes:
        ae.add_supported_context(abstract_syntax, ALL_TRANSFER_SYNTAXES)

    return ae


def main() -> int:
    """
    Main entry point for DICOM listener.
    
    Starts DICOM SCP server and periodic study scanner.
    """
    ap = argparse.ArgumentParser(
        description="Heimdallr DICOM C-STORE listener with automatic upload"
    )
    
    # All arguments have defaults from package settings.
    ap.add_argument("--ae", default=settings.DICOM_AE_TITLE, help="DICOM AE title")
    ap.add_argument("--port", type=int, default=settings.DICOM_PORT, help="DICOM port")
    ap.add_argument("--incoming-dir", default=str(settings.DICOM_INCOMING_DIR), help="Incoming DICOM directory")
    ap.add_argument("--failed-dir", default=str(settings.DICOM_FAILED_DIR), help="Failed uploads archive")
    ap.add_argument("--state-dir", default=str(settings.DICOM_STATE_DIR), help="State directory")
    ap.add_argument("--idle-seconds", type=int, default=settings.DICOM_IDLE_SECONDS, help="Study idle timeout")
    ap.add_argument("--scan-seconds", type=int, default=settings.DICOM_SCAN_SECONDS, help="Scan interval")
    ap.add_argument("--upload-url", default=settings.DICOM_UPLOAD_URL, help="Upload endpoint URL")
    ap.add_argument("--upload-token", default=settings.DICOM_UPLOAD_TOKEN, help="Optional auth token")
    ap.add_argument("--upload-timeout", type=int, default=settings.DICOM_UPLOAD_TIMEOUT, help="Upload timeout (seconds)")
    ap.add_argument("--upload-retries", type=int, default=settings.DICOM_UPLOAD_RETRIES, help="Upload retry attempts")
    ap.add_argument("--upload-backoff", type=int, default=settings.DICOM_UPLOAD_BACKOFF, help="Retry backoff (seconds)")
    ap.add_argument(
        "--handoff-mode",
        choices=["local_prepare", "http_upload"],
        default=settings.DICOM_HANDOFF_MODE,
        help="How to deliver completed studies: local_prepare (default) or http_upload",
    )
    args = ap.parse_args()

    # Initialize listener
    listener = HeimdallrDicomListener(
        incoming_dir=Path(args.incoming_dir),
        failed_dir=Path(args.failed_dir),
        state_dir=Path(args.state_dir),
        idle_seconds=args.idle_seconds,
        upload_url=args.upload_url,
        upload_token=args.upload_token,
        upload_timeout=args.upload_timeout,
        upload_retries=args.upload_retries,
        upload_backoff=args.upload_backoff,
        handoff_mode=args.handoff_mode,
        upload_staging_dir=Path(settings.UPLOAD_DIR),
    )

    # Setup signal handlers for graceful shutdown
    def _sig_handler(signum, frame) -> None:
        print("\nShutting down...")
        listener.stop()

    signal.signal(signal.SIGINT, _sig_handler)
    signal.signal(signal.SIGTERM, _sig_handler)

    # Configure event handlers
    handlers = [(evt.EVT_C_STORE, listener.on_c_store)]

    # Start DICOM SCP server
    ae = build_ae(args.ae)
    scp = ae.start_server(("", args.port), block=False, evt_handlers=handlers)

    print(f"Heimdallr DICOM Listener started")
    print(f"  AE Title: {args.ae}")
    print(f"  Port: {args.port}")
    print(f"  Handoff mode: {args.handoff_mode}")
    if args.handoff_mode == "http_upload":
        print(f"  Upload URL: {args.upload_url}")
    else:
        print(f"  Prepare spool: {settings.UPLOAD_DIR}")
    print(f"  Idle timeout: {args.idle_seconds}s")
    print(f"Waiting for DICOM connections...")

    try:
        # Main loop: periodically scan for idle studies
        while not listener._stop:
            listener.scan_and_flush()
            time.sleep(args.scan_seconds)
    finally:
        scp.shutdown()
        print("Server stopped")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
