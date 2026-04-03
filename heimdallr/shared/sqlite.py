"""SQLite helpers for the Heimdallr operational store."""

from __future__ import annotations

import sqlite3

from . import settings


def connect(*, check_same_thread: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(settings.DB_PATH, check_same_thread=check_same_thread)
    conn.row_factory = sqlite3.Row
    return conn
