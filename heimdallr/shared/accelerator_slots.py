"""Cross-service accelerator admission slots backed by operating-system locks."""

from contextlib import contextmanager
import fcntl
from pathlib import Path
import time
from typing import Iterator

from heimdallr.shared import settings


def _slot_paths() -> list[Path]:
    count = int(settings.ACCELERATOR_TASK_SLOTS)
    if count <= 0:
        return []
    root = settings.RUNTIME_DIR / "locks" / "accelerator_slots"
    root.mkdir(parents=True, exist_ok=True)
    return [root / f"slot_{index:02d}.lock" for index in range(count)]


@contextmanager
def accelerator_slot(*, enabled: bool, poll_seconds: float = 0.1) -> Iterator[int | None]:
    """Acquire one host-wide slot, or behave as a no-op when admission is disabled."""
    paths = _slot_paths() if enabled else []
    if not paths:
        yield None
        return

    handle = None
    slot = None
    while handle is None:
        for index, path in enumerate(paths):
            candidate = path.open("a+")
            try:
                fcntl.flock(candidate.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                candidate.close()
                continue
            handle = candidate
            slot = index
            break
        if handle is None:
            time.sleep(max(0.01, poll_seconds))

    try:
        yield slot
    finally:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()
