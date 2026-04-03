"""Legacy ASGI entrypoint preserved as a thin wrapper."""

from __future__ import annotations

import uvicorn

from heimdallr.control_plane.app import app
from heimdallr.shared import settings


if __name__ == "__main__":
    uvicorn.run(app, host=settings.SERVER_HOST, port=settings.SERVER_PORT, reload=False)
