"""Helpers for atomic spool writes and claims."""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

TEMP_SUFFIX = ".part"
CLAIM_SUFFIX = ".working"


def atomic_write_bytes(target_path: Path, payload: bytes) -> Path:
    """Write bytes to disk via a temporary file and atomic rename."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.parent / f".{target_path.name}.{uuid.uuid4().hex}{TEMP_SUFFIX}"
    with open(temp_path, "wb") as handle:
        handle.write(payload)
    temp_path.replace(target_path)
    return target_path


def atomic_copy_stream(target_path: Path, source_stream) -> Path:
    """Copy a stream to disk via a temporary file and atomic rename."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.parent / f".{target_path.name}.{uuid.uuid4().hex}{TEMP_SUFFIX}"
    with open(temp_path, "wb") as handle:
        shutil.copyfileobj(source_stream, handle)
    temp_path.replace(target_path)
    return target_path


def claim_path(path: Path, suffix: str = CLAIM_SUFFIX) -> Path:
    """Claim a spooled file by atomically renaming it to a working suffix."""
    if path.name.endswith(suffix):
        return path
    claimed = path.with_name(f"{path.name}{suffix}")
    path.replace(claimed)
    return claimed


def unclaim_path(path: Path, suffix: str = CLAIM_SUFFIX) -> Path:
    """Return the original logical name for a claimed spool file."""
    if path.name.endswith(suffix):
        return path.with_name(path.name[: -len(suffix)])
    return path
