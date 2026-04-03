"""FastAPI dependencies shared by control-plane routers."""

from __future__ import annotations

from collections.abc import Generator

from .sqlite import connect
from .store import ensure_schema


def get_db() -> Generator:
    """Yield a SQLite connection scoped to one request."""
    conn = None
    try:
        conn = connect(check_same_thread=False)
        ensure_schema(conn)
        yield conn
    finally:
        if conn:
            conn.close()
