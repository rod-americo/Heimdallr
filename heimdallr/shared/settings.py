"""Runtime settings for the modular Heimdallr package."""

from __future__ import annotations

import json
import os
import platform
import sys
from pathlib import Path
from zoneinfo import ZoneInfo



def _parse_csv_env(name: str, default: str) -> list[str]:
    raw = os.getenv(name, default)
    parts = [part.strip() for part in raw.split(",")]
    return [part for part in parts if part]


def _load_json_config(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _get_nested_config(config: dict, *keys: str):
    current = config
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _config_int(env_name: str, config: dict, keys: tuple[str, ...], default: int) -> int:
    explicit = os.getenv(env_name)
    if explicit is not None:
        return int(explicit)
    configured = _get_nested_config(config, *keys)
    if configured is None:
        return default
    return int(configured)


def _config_float(env_name: str, config: dict, keys: tuple[str, ...], default: float) -> float:
    explicit = os.getenv(env_name)
    if explicit is not None:
        return float(explicit)
    configured = _get_nested_config(config, *keys)
    if configured is None:
        return default
    return float(configured)


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_RUNTIME_PYTHON = str(Path(sys.executable))
DEFAULT_BIN_DIR = BASE_DIR / "bin"
DEFAULT_TOTALSEG_VENV_DIR = BASE_DIR / ".venv"
DEFAULT_TOTALSEG_BIN_DIR = DEFAULT_TOTALSEG_VENV_DIR / "bin"
DEFAULT_METRICS_PYTHON = str(
    (DEFAULT_TOTALSEG_BIN_DIR / "python")
    if (DEFAULT_TOTALSEG_BIN_DIR / "python").exists()
    else Path(sys.executable)
)



RUNTIME_DIR = BASE_DIR / "runtime"
CONFIG_DIR = BASE_DIR / "config"
INTAKE_DIR = RUNTIME_DIR / "intake"
QUEUE_DIR = RUNTIME_DIR / "queue"
STUDIES_DIR = RUNTIME_DIR / "studies"

UPLOAD_DIR = INTAKE_DIR / "uploads"
UPLOAD_FAILED_DIR = INTAKE_DIR / "uploads_failed"
DICOM_DIR = INTAKE_DIR / "dicom"
INPUT_DIR = QUEUE_DIR / "pending"
SEGMENTATION_DIR = QUEUE_DIR / "active"
ERROR_DIR = QUEUE_DIR / "failed"
OUTPUT_DIR = STUDIES_DIR
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = RUNTIME_DIR

DB_DIR = BASE_DIR / "database"
DB_PATH = DB_DIR / "dicom.db"

SERVER_HOST = os.getenv("HEIMDALLR_SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("HEIMDALLR_SERVER_PORT", "8001"))
SERVER_TITLE = os.getenv("HEIMDALLR_SERVER_TITLE", "Heimdallr - Radiology AI Pipeline")
TIMEZONE = os.getenv("HEIMDALLR_TIMEZONE", "America/Sao_Paulo")
LOCAL_TZ = ZoneInfo(TIMEZONE)
INTAKE_PIPELINE_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_INTAKE_PIPELINE_CONFIG",
        str(CONFIG_DIR / "intake_pipeline.json"),
    )
)
INTAKE_PIPELINE_CONFIG = _load_json_config(INTAKE_PIPELINE_CONFIG_PATH)

DICOM_AE_TITLE = os.getenv("HEIMDALLR_AE_TITLE", "HEIMDALLR")
DICOM_PORT = int(os.getenv("HEIMDALLR_DICOM_PORT", "11114"))
DICOM_INCOMING_DIR = Path(os.getenv("HEIMDALLR_INCOMING_DIR", str(DICOM_DIR / "incoming")))
DICOM_FAILED_DIR = Path(os.getenv("HEIMDALLR_FAILED_DIR", str(DICOM_DIR / "failed")))
DICOM_STATE_DIR = Path(os.getenv("HEIMDALLR_STATE_DIR", str(DICOM_DIR / "state")))
DICOM_IDLE_SECONDS = _config_int(
    "HEIMDALLR_IDLE_SECONDS",
    INTAKE_PIPELINE_CONFIG,
    ("dicom_listener", "idle_seconds"),
    30,
)
DICOM_SCAN_SECONDS = _config_int(
    "HEIMDALLR_SCAN_SECONDS",
    INTAKE_PIPELINE_CONFIG,
    ("dicom_listener", "scan_seconds"),
    5,
)
DICOM_UPLOAD_URL = os.getenv("HEIMDALLR_UPLOAD_URL", f"http://127.0.0.1:{SERVER_PORT}/upload")
DICOM_UPLOAD_TOKEN = os.getenv("HEIMDALLR_UPLOAD_TOKEN")
DICOM_UPLOAD_TIMEOUT = int(os.getenv("HEIMDALLR_UPLOAD_TIMEOUT", "120"))
DICOM_UPLOAD_RETRIES = int(os.getenv("HEIMDALLR_UPLOAD_RETRIES", "3"))
DICOM_UPLOAD_BACKOFF = int(os.getenv("HEIMDALLR_UPLOAD_BACKOFF", "5"))
DICOM_HANDOFF_MODE = os.getenv("HEIMDALLR_DICOM_HANDOFF_MODE", "local_prepare")
PREPARE_SCAN_INTERVAL = _config_int(
    "HEIMDALLR_PREPARE_SCAN_INTERVAL",
    INTAKE_PIPELINE_CONFIG,
    ("prepare_watchdog", "scan_interval_seconds"),
    2,
)
PREPARE_STABLE_AGE_SECONDS = _config_int(
    "HEIMDALLR_PREPARE_STABLE_AGE_SECONDS",
    INTAKE_PIPELINE_CONFIG,
    ("prepare_watchdog", "stable_age_seconds"),
    5,
)
PREPARE_MIN_SERIES_IMAGES = int(os.getenv("HEIMDALLR_PREPARE_MIN_SERIES_IMAGES", "120"))
TOTALSEGMENTATOR_LICENSE = os.getenv("TOTALSEGMENTATOR_LICENSE")
MAX_PARALLEL_CASES = int(os.getenv("HEIMDALLR_MAX_PARALLEL_CASES", "3"))
SEGMENTATION_SCAN_INTERVAL = int(os.getenv("HEIMDALLR_SEGMENTATION_SCAN_INTERVAL", "2"))
VERBOSE_CONSOLE = os.getenv("HEIMDALLR_VERBOSE_CONSOLE", "false").lower() == "true"
METRICS_MODULES = _parse_csv_env(
    "HEIMDALLR_METRICS_MODULES",
    "body_regions,abdominal_organs,renal_stone_burden,l3_sarcopenia,cerebral_hemorrhage,l1_bmd,emphysema",
)
METRICS_ENABLE_OVERLAYS = os.getenv("HEIMDALLR_METRICS_ENABLE_OVERLAYS", "true").lower() == "true"
METRICS_PYTHON = os.getenv("HEIMDALLR_METRICS_PYTHON", DEFAULT_METRICS_PYTHON)
METRICS_SCAN_INTERVAL = int(os.getenv("HEIMDALLR_METRICS_SCAN_INTERVAL", "2"))
METRICS_PIPELINE_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_METRICS_PIPELINE_CONFIG",
        str(CONFIG_DIR / "metrics_pipeline.json"),
    )
)
METRICS_PIPELINE_PROFILE = os.getenv("HEIMDALLR_METRICS_PIPELINE_PROFILE")
DICOM_EGRESS_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_DICOM_EGRESS_CONFIG",
        str(CONFIG_DIR / "dicom_egress.json"),
    )
)
DICOM_EGRESS_SCAN_INTERVAL = int(os.getenv("HEIMDALLR_DICOM_EGRESS_SCAN_INTERVAL", "2"))
INTEGRATION_DISPATCH_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_INTEGRATION_DISPATCH_CONFIG",
        str(CONFIG_DIR / "integration_dispatch.json"),
    )
)
INTEGRATION_DISPATCH_CONFIG = _load_json_config(INTEGRATION_DISPATCH_CONFIG_PATH)
INTEGRATION_DISPATCH_SCAN_INTERVAL = _config_int(
    "HEIMDALLR_INTEGRATION_DISPATCH_SCAN_INTERVAL",
    INTEGRATION_DISPATCH_CONFIG,
    ("scan_interval_seconds",),
    5,
)
SPACE_MANAGER_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_SPACE_MANAGER_CONFIG",
        str(CONFIG_DIR / "space_manager.json"),
    )
)
SPACE_MANAGER_CONFIG = _load_json_config(SPACE_MANAGER_CONFIG_PATH)
SPACE_MANAGER_SCAN_INTERVAL = _config_int(
    "HEIMDALLR_SPACE_MANAGER_SCAN_INTERVAL",
    SPACE_MANAGER_CONFIG,
    ("scan_interval_seconds",),
    60,
)
SPACE_MANAGER_USAGE_THRESHOLD_PERCENT = _config_float(
    "HEIMDALLR_SPACE_MANAGER_USAGE_THRESHOLD_PERCENT",
    SPACE_MANAGER_CONFIG,
    ("usage_threshold_percent",),
    80.0,
)
SERIES_SELECTION_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_SERIES_SELECTION_CONFIG",
        str(CONFIG_DIR / "series_selection.json"),
    )
)
SERIES_SELECTION_PROFILE = os.getenv("HEIMDALLR_SERIES_SELECTION_PROFILE")
SEGMENTATION_PIPELINE_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_SEGMENTATION_PIPELINE_CONFIG",
        str(CONFIG_DIR / "segmentation_pipeline.json"),
    )
)
SEGMENTATION_PIPELINE_PROFILE = os.getenv("HEIMDALLR_SEGMENTATION_PIPELINE_PROFILE")
PRESENTATION_CONFIG_PATH = Path(
    os.getenv(
        "HEIMDALLR_PRESENTATION_CONFIG",
        str(CONFIG_DIR / "presentation.json"),
    )
)
PRESENTATION_CONFIG = _load_json_config(PRESENTATION_CONFIG_PATH)
PATIENT_NAME_PROFILE = (
    PRESENTATION_CONFIG.get("patient_name", {}).get("profile", "default")
)
ARTIFACTS_LOCALE = (
    PRESENTATION_CONFIG.get("artifacts", {}).get("locale")
    or os.getenv("HEIMDALLR_ARTIFACTS_LOCALE")
    or os.getenv("HEIMDALLR_LOCALE")
    or "en_US"
)
TUI_LOCALE = (
    PRESENTATION_CONFIG.get("tui", {}).get("locale")
    or ARTIFACTS_LOCALE
    or os.getenv("HEIMDALLR_LOCALE")
    or "en_US"
)

UPLOADER_DEFAULT_SERVER = os.getenv("HEIMDALLR_UPLOADER_SERVER", "http://thor:8001/upload")
MEDGEMMA_SERVICE_URL = os.getenv("MEDGEMMA_SERVICE_URL", "http://localhost:8004/analyze")
ANTHROPIC_SERVICE_URL = os.getenv("ANTHROPIC_SERVICE_URL", "http://localhost:8101/analyze")
TOTALSEGMENTATOR_SERVICE_URL = os.getenv("TOTALSEGMENTATOR_SERVICE_URL", "http://localhost:8005/process")


def local_now():
    """Return the current timestamp in the configured operational timezone."""
    from datetime import datetime

    return datetime.now(LOCAL_TZ)


def local_timestamp(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Render the current operational time using the configured timezone."""
    return local_now().strftime(fmt)


def configure_service_stdio() -> None:
    """Force line-buffered stdout/stderr for long-running service processes.

    Resident workers mainly use ``print()`` for operational progress. Under
    systemd these streams can stay block-buffered and only flush on restart,
    which makes `journalctl`/`skuld logs` misleading during live monitoring.
    Jobs that emit structured JSON should not call this helper.
    """
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(line_buffering=True)


def _resolve_binary(env_name: str, bundled_name: str, fallback_name: str) -> str:
    explicit = os.getenv(env_name)
    if explicit:
        return explicit

    system = platform.system().lower()
    machine = platform.machine().lower()
    platform_candidates: list[Path] = []

    if system == "darwin" and machine in {"arm64", "aarch64"}:
        platform_candidates.append(DEFAULT_BIN_DIR / "darwin-arm64" / bundled_name)
    elif system == "linux" and machine in {"x86_64", "amd64"}:
        platform_candidates.append(DEFAULT_BIN_DIR / "linux-amd64" / bundled_name)

    platform_candidates.append(DEFAULT_BIN_DIR / bundled_name)
    for bundled_path in platform_candidates:
        if bundled_path.exists():
            return str(bundled_path)
    return fallback_name


DCM2NIIX_BIN = _resolve_binary("HEIMDALLR_DCM2NIIX_BIN", "dcm2niix", "dcm2niix")
DCMCJPEG_BIN = _resolve_binary("HEIMDALLR_DCMCJPEG_BIN", "dcmcjpeg", "dcmcjpeg")
TOTALSEG_BIN_DIR = Path(
    os.getenv(
        "HEIMDALLR_TOTALSEG_BIN_DIR",
        str(DEFAULT_TOTALSEG_BIN_DIR if DEFAULT_TOTALSEG_BIN_DIR.exists() else Path(DEFAULT_RUNTIME_PYTHON).parent),
    )
)
TOTALSEG_GET_PHASE_BIN = os.getenv(
    "HEIMDALLR_TOTALSEG_GET_PHASE_BIN",
    str(TOTALSEG_BIN_DIR / "totalseg_get_phase"),
)
TOTALSEGMENTATOR_BIN = os.getenv(
    "HEIMDALLR_TOTALSEGMENTATOR_BIN",
    str(TOTALSEG_BIN_DIR / "TotalSegmentator"),
)


def ensure_directories() -> None:
    """Create the runtime directories used by modular entrypoints."""
    for directory in (
        RUNTIME_DIR,
        INTAKE_DIR,
        QUEUE_DIR,
        STUDIES_DIR,
        UPLOAD_DIR,
        UPLOAD_FAILED_DIR,
        DICOM_DIR,
        OUTPUT_DIR,
        INPUT_DIR,
        SEGMENTATION_DIR,
        ERROR_DIR,
        STATIC_DIR,
        DATA_DIR,
        DB_DIR,
        DICOM_INCOMING_DIR,
        DICOM_FAILED_DIR,
        DICOM_STATE_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)
