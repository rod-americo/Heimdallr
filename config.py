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
Heimdallr Configuration Module

Centralized configuration for all Heimdallr services:
- Server (FastAPI upload/dashboard)
- DICOM Listener (C-STORE SCP)
- Processing Daemon (run.py)
- Uploader Client

All settings can be overridden via environment variables.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================
# BASE PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads"
NII_DIR = BASE_DIR / "nii"
OUTPUT_DIR = BASE_DIR / "output"
INPUT_DIR = BASE_DIR / "input"
ERROR_DIR = BASE_DIR / "errors"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = BASE_DIR / "data"

# ============================================================
# SERVER CONFIGURATION (server.py)
# ============================================================

# FastAPI server settings
SERVER_HOST = os.getenv("HEIMDALLR_SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("HEIMDALLR_SERVER_PORT", "8001"))
SERVER_TITLE = "Heimdallr - Radiology AI Pipeline"

# ============================================================
# DICOM LISTENER CONFIGURATION (dicom_listener.py)
# ============================================================

# DICOM network settings
DICOM_AE_TITLE = os.getenv("HEIMDALLR_AE_TITLE", "HEIMDALLR")
DICOM_PORT = int(os.getenv("HEIMDALLR_DICOM_PORT", "11112"))

# DICOM storage directories
DICOM_INCOMING_DIR = Path(os.getenv("HEIMDALLR_INCOMING_DIR", str(DATA_DIR / "incoming_dicom")))
DICOM_SENT_DIR = Path(os.getenv("HEIMDALLR_SENT_DIR", str(DATA_DIR / "sent")))
DICOM_FAILED_DIR = Path(os.getenv("HEIMDALLR_FAILED_DIR", str(DATA_DIR / "failed")))
DICOM_STATE_DIR = Path(os.getenv("HEIMDALLR_STATE_DIR", str(DATA_DIR / "state")))

# Study completion timing
DICOM_IDLE_SECONDS = int(os.getenv("HEIMDALLR_IDLE_SECONDS", "30"))  # Time without new images to close study
DICOM_SCAN_SECONDS = int(os.getenv("HEIMDALLR_SCAN_SECONDS", "5"))   # Scan interval for idle studies

# Upload settings for DICOM listener
DICOM_UPLOAD_URL = os.getenv("HEIMDALLR_UPLOAD_URL", f"http://127.0.0.1:{SERVER_PORT}/upload")
DICOM_UPLOAD_TOKEN = os.getenv("HEIMDALLR_UPLOAD_TOKEN")  # Optional bearer token
DICOM_UPLOAD_TIMEOUT = int(os.getenv("HEIMDALLR_UPLOAD_TIMEOUT", "120"))
DICOM_UPLOAD_RETRIES = int(os.getenv("HEIMDALLR_UPLOAD_RETRIES", "3"))
DICOM_UPLOAD_BACKOFF = int(os.getenv("HEIMDALLR_UPLOAD_BACKOFF", "5"))  # Seconds between retries

# ============================================================
# PROCESSING CONFIGURATION (run.py)
# ============================================================

# TotalSegmentator license (required - set in .env file)
TOTALSEGMENTATOR_LICENSE = os.getenv("TOTALSEGMENTATOR_LICENSE")
if not TOTALSEGMENTATOR_LICENSE:
    raise ValueError(
        "TOTALSEGMENTATOR_LICENSE environment variable is required. "
        "Please create a .env file with your license key (see .env.example)"
    )

# Parallel processing
MAX_PARALLEL_CASES = int(os.getenv("HEIMDALLR_MAX_PARALLEL_CASES", "3"))
PROCESSING_SCAN_INTERVAL = int(os.getenv("HEIMDALLR_PROCESSING_SCAN_INTERVAL", "2"))  # Seconds

# Logging configuration
VERBOSE_CONSOLE = os.getenv("HEIMDALLR_VERBOSE_CONSOLE", "false").lower() == "true"  # False = logs to files, True = logs to console

# ============================================================
# UPLOADER CLIENT CONFIGURATION (uploader.py)
# ============================================================

# Default upload server for CLI client
UPLOADER_DEFAULT_SERVER = os.getenv("HEIMDALLR_UPLOADER_SERVER", "http://thor:8001/upload")

# ============================================================
# PREPARE SCRIPT CONFIGURATION (prepare.py)
# ============================================================

# DICOM to NIfTI conversion settings
PREPARE_SCRIPT = BASE_DIR / "prepare.py"

# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_DIR = BASE_DIR / "database"
DB_PATH = DB_DIR / "dicom.db"

# ============================================================
# HELPER FUNCTIONS
# ============================================================

def ensure_directories():
    """Create all required directories if they don't exist."""
    dirs = [
        UPLOAD_DIR, NII_DIR, OUTPUT_DIR, INPUT_DIR, ERROR_DIR,
        STATIC_DIR, DATA_DIR, DB_DIR,
        DICOM_INCOMING_DIR, DICOM_SENT_DIR, DICOM_FAILED_DIR, DICOM_STATE_DIR
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    # Print current configuration when run directly
    print("=== Heimdallr Configuration ===\n")
    
    print("[Server]")
    print(f"  Host: {SERVER_HOST}")
    print(f"  Port: {SERVER_PORT}")
    
    print("\n[DICOM Listener]")
    print(f"  AE Title: {DICOM_AE_TITLE}")
    print(f"  Port: {DICOM_PORT}")
    print(f"  Upload URL: {DICOM_UPLOAD_URL}")
    print(f"  Idle Timeout: {DICOM_IDLE_SECONDS}s")
    
    print("\n[Processing]")
    print(f"  Max Parallel Cases: {MAX_PARALLEL_CASES}")
    print(f"  License: {TOTALSEGMENTATOR_LICENSE}")
    
    print("\n[Paths]")
    print(f"  Base: {BASE_DIR}")
    print(f"  Input: {INPUT_DIR}")
    print(f"  Output: {OUTPUT_DIR}")
    print(f"  DICOM Incoming: {DICOM_INCOMING_DIR}")
    
    ensure_directories()
    print("\n✓ All directories created/verified")
