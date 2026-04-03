"""Module entrypoint for the Heimdallr control plane."""

from __future__ import annotations

import uvicorn

from .app import app
from ..shared import settings


def main() -> int:
    uvicorn.run(app, host=settings.SERVER_HOST, port=settings.SERVER_PORT, reload=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
