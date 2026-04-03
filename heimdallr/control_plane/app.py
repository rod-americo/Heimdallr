"""ASGI application factory for the Heimdallr control plane."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from ..shared import settings
from .routers.dashboard import router as dashboard_router
from .routers.patients import router as patients_router
from .routers.proxy import router as proxy_router
from .routers.upload import router as upload_router


def create_app() -> FastAPI:
    """Build the operational API application."""
    settings.ensure_directories()

    app = FastAPI(title=settings.SERVER_TITLE)
    app.include_router(dashboard_router)
    app.include_router(upload_router)
    app.include_router(patients_router)
    app.include_router(proxy_router)

    if settings.STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=settings.STATIC_DIR), name="static")

    return app


app = create_app()

