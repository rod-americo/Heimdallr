"""Helpers for stable study-level manifest digests."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path


def build_study_manifest_digest(
    root_dir: Path,
    *,
    study_uid: str,
    calling_aet: str | None = None,
    instance_count: int | None = None,
    ignored_names: set[str] | None = None,
) -> str:
    """Build a stable digest for a study payload based on its file tree."""
    digest = sha256()
    digest.update(str(study_uid or "").strip().encode("utf-8"))
    digest.update(b"\n")
    digest.update(str(calling_aet or "").strip().encode("utf-8"))
    digest.update(b"\n")
    digest.update(str(int(instance_count or 0)).encode("utf-8"))
    digest.update(b"\n")

    ignored = set(ignored_names or set())
    file_entries: list[tuple[str, int]] = []
    for path in root_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.name in ignored:
            continue
        try:
            size = int(path.stat().st_size)
        except OSError:
            size = -1
        file_entries.append((path.relative_to(root_dir).as_posix(), size))

    for relative_path, size in sorted(file_entries):
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\t")
        digest.update(str(size).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()
