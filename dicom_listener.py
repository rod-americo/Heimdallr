#!/usr/bin/env python3
"""
Heimdallr DICOM Listener (dicom_listener.py)

DICOM C-STORE SCP (Service Class Provider) that:
1. Receives DICOM images from modalities/PACS via DICOM protocol
2. Groups images by StudyInstanceUID
3. Automatically closes studies after idle timeout (no new images)
4. Zips completed studies and uploads to Heimdallr server
5. Handles upload retries with exponential backoff

Usage:
    python dicom_listener.py

All configuration is centralized in config.py and can be overridden via environment variables.
"""

from __future__ import annotations

import argparse
import io
import os
import shutil
import signal
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import requests
from pydicom.dataset import Dataset
from pynetdicom import AE, evt
from pynetdicom.sop_class import (
    CTImageStorage,
    MRImageStorage,
    EnhancedCTImageStorage,
    EnhancedMRImageStorage,
    SecondaryCaptureImageStorage,
    UltrasoundImageStorage,
    DigitalXRayImageStorageForPresentation,
    DigitalXRayImageStorageForProcessing,
    ComputedRadiographyImageStorage,
    PositronEmissionTomographyImageStorage,
)

# Import centralized configuration
import config


@dataclass
class StudyState:
    """
    Tracks the state of an active DICOM study being received.
    
    Attributes:
        study_uid: StudyInstanceUID (sanitized for filesystem)
        path: Directory where DICOM files are stored
        last_update_ts: Timestamp of last received image
        locked: Whether study is currently being processed/uploaded
    """
    study_uid: str
    path: Path
    last_update_ts: float
    locked: bool = False


def safe_mkdir(p: Path) -> None:
    """Create directory and all parent directories if they don't exist."""
    p.mkdir(parents=True, exist_ok=True)


def now() -> float:
    """Get current Unix timestamp."""
    return time.time()


def sanitize_filename(s: str) -> str:
    """
    Sanitize string for use as filename/directory name.
    
    Removes special characters and limits length to prevent filesystem issues.
    """
    return "".join(c for c in s if c.isalnum() or c in ("-", "_", "."))[:200] or "unknown"


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


def zip_study(study_dir: Path) -> bytes:
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
        sent_dir: Path,
        failed_dir: Path,
        state_dir: Path,
        idle_seconds: int,
        upload_url: str,
        upload_token: Optional[str],
        upload_timeout: int,
        upload_retries: int,
        upload_backoff: int,
    ) -> None:
        """
        Initialize DICOM listener.
        
        Args:
            incoming_dir: Directory for receiving DICOM files
            sent_dir: Archive for successfully uploaded studies
            failed_dir: Archive for failed uploads
            state_dir: Directory for persistent state (future use)
            idle_seconds: Time without new images to close study
            upload_url: Heimdallr server upload endpoint
            upload_token: Optional authentication token
            upload_timeout: HTTP request timeout
            upload_retries: Number of upload attempts
            upload_backoff: Seconds between retry attempts
        """
        self.incoming_dir = incoming_dir
        self.sent_dir = sent_dir
        self.failed_dir = failed_dir
        self.state_dir = state_dir

        self.idle_seconds = idle_seconds

        self.upload_url = upload_url
        self.upload_token = upload_token
        self.upload_timeout = upload_timeout
        self.upload_retries = upload_retries
        self.upload_backoff = upload_backoff

        # Active studies being received
        self.studies: Dict[str, StudyState] = {}
        self._stop = False

        # Ensure all directories exist
        for p in (incoming_dir, sent_dir, failed_dir, state_dir):
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

            study_uid_s = sanitize_filename(study_uid)
            study_dir = self.incoming_dir / study_uid_s
            safe_mkdir(study_dir)

            # Write DICOM file to disk
            write_instance(ds, study_dir)

            # Update study state
            st = self.studies.get(study_uid_s)
            if not st:
                # New study
                st = StudyState(study_uid=study_uid_s, path=study_dir, last_update_ts=now())
                self.studies[study_uid_s] = st
            else:
                # Existing study: update timestamp
                st.last_update_ts = now()

            # Return success status
            return 0x0000
            
        except Exception:
            # Unexpected error: return processing failure status
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
                # Create ZIP archive
                zip_bytes = zip_study(st.path)

                # Attempt upload with retries
                ok = False
                last_exc: Optional[Exception] = None
                
                for attempt in range(1, self.upload_retries + 1):
                    try:
                        resp = upload_zip(
                            zip_bytes, 
                            self.upload_url, 
                            self.upload_token, 
                            self.upload_timeout
                        )
                        
                        if 200 <= resp.status_code < 300:
                            ok = True
                            break
                        else:
                            # Server returned error: wait and retry
                            time.sleep(self.upload_backoff)
                            
                    except Exception as e:
                        last_exc = e
                        time.sleep(self.upload_backoff)

                # Generate timestamped filename for archive
                ts = time.strftime("%Y%m%d%H%M%S")
                zip_name = f"{ts}_{study_uid}.zip"
                
                if ok:
                    # Upload successful: archive ZIP and clean up raw DICOM
                    sent_path = self.sent_dir / zip_name
                    sent_path.write_bytes(zip_bytes)
                    
                    # Remove raw DICOM files
                    shutil.rmtree(st.path, ignore_errors=True)
                    
                    # Remove from active studies
                    self.studies.pop(study_uid, None)
                    
                    print(f"✓ Study {study_uid} uploaded successfully")
                    
                else:
                    # Upload failed: save to failed directory for manual review
                    fail_path = self.failed_dir / zip_name
                    fail_path.write_bytes(zip_bytes)
                    
                    # Remove from active studies (keep raw DICOM for investigation)
                    self.studies.pop(study_uid, None)
                    
                    print(f"✗ Study {study_uid} upload failed after {self.upload_retries} attempts")
                    if last_exc:
                        print(f"  Last error: {last_exc}")
                        
            finally:
                st.locked = False


def build_ae(ae_title: str) -> AE:
    """
    Build DICOM Application Entity with supported SOP classes.
    
    Configures which DICOM modalities/image types can be received.
    
    Args:
        ae_title: DICOM AE title for this listener
    
    Returns:
        Configured Application Entity
    """
    ae = AE(ae_title=ae_title)

    # Add support for common imaging modalities
    storage_sop_classes = [
        CTImageStorage,                              # CT scans
        EnhancedCTImageStorage,                      # Enhanced CT
        MRImageStorage,                              # MR scans
        EnhancedMRImageStorage,                      # Enhanced MR
        SecondaryCaptureImageStorage,                # Screenshots, derived images
        UltrasoundImageStorage,                      # Ultrasound
        ComputedRadiographyImageStorage,             # CR (computed radiography)
        DigitalXRayImageStorageForPresentation,      # DX presentation
        DigitalXRayImageStorageForProcessing,        # DX processing
        PositronEmissionTomographyImageStorage,      # PET scans
    ]
    
    for sop in storage_sop_classes:
        ae.add_supported_context(sop)

    return ae


def main() -> int:
    """
    Main entry point for DICOM listener.
    
    Starts DICOM SCP server and periodic study scanner.
    """
    ap = argparse.ArgumentParser(
        description="Heimdallr DICOM C-STORE listener with automatic upload"
    )
    
    # All arguments have defaults from config.py
    ap.add_argument("--ae", default=config.DICOM_AE_TITLE, help="DICOM AE title")
    ap.add_argument("--port", type=int, default=config.DICOM_PORT, help="DICOM port")
    ap.add_argument("--incoming-dir", default=str(config.DICOM_INCOMING_DIR), help="Incoming DICOM directory")
    ap.add_argument("--sent-dir", default=str(config.DICOM_SENT_DIR), help="Sent studies archive")
    ap.add_argument("--failed-dir", default=str(config.DICOM_FAILED_DIR), help="Failed uploads archive")
    ap.add_argument("--state-dir", default=str(config.DICOM_STATE_DIR), help="State directory")
    ap.add_argument("--idle-seconds", type=int, default=config.DICOM_IDLE_SECONDS, help="Study idle timeout")
    ap.add_argument("--scan-seconds", type=int, default=config.DICOM_SCAN_SECONDS, help="Scan interval")
    ap.add_argument("--upload-url", default=config.DICOM_UPLOAD_URL, help="Upload endpoint URL")
    ap.add_argument("--upload-token", default=config.DICOM_UPLOAD_TOKEN, help="Optional auth token")
    ap.add_argument("--upload-timeout", type=int, default=config.DICOM_UPLOAD_TIMEOUT, help="Upload timeout (seconds)")
    ap.add_argument("--upload-retries", type=int, default=config.DICOM_UPLOAD_RETRIES, help="Upload retry attempts")
    ap.add_argument("--upload-backoff", type=int, default=config.DICOM_UPLOAD_BACKOFF, help="Retry backoff (seconds)")
    
    args = ap.parse_args()

    # Initialize listener
    listener = HeimdallrDicomListener(
        incoming_dir=Path(args.incoming_dir),
        sent_dir=Path(args.sent_dir),
        failed_dir=Path(args.failed_dir),
        state_dir=Path(args.state_dir),
        idle_seconds=args.idle_seconds,
        upload_url=args.upload_url,
        upload_token=args.upload_token,
        upload_timeout=args.upload_timeout,
        upload_retries=args.upload_retries,
        upload_backoff=args.upload_backoff,
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
    print(f"  Upload URL: {args.upload_url}")
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