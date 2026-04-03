"""Dashboard and utility routes for the control plane."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import FileResponse, HTMLResponse

from ...shared import settings

router = APIRouter(tags=["dashboard"])


@router.get("/api/tools/uploader")
async def download_uploader():
    """Expose the CLI uploader script through the web UI."""
    script_path = settings.BASE_DIR / "clients" / "uploader.py"
    return FileResponse(
        path=script_path,
        filename="uploader.py",
        media_type="text/x-python",
    )


@router.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the dashboard shell or a simple fallback page."""
    index_path = settings.STATIC_DIR / "index.html"
    if index_path.exists():
        return HTMLResponse(content=index_path.read_text(encoding="utf-8"))

    return HTMLResponse(
        content="""
        <html>
            <head><title>Heimdallr</title></head>
            <body style="font-family: sans-serif; padding: 40px; background: #1a1a2e; color: #eee;">
                <h1>🔭 Heimdallr</h1>
                <p>Dashboard not found. Please create <code>static/index.html</code></p>
                <p><a href="/docs" style="color: #4cc9f0;">API Documentation -></a></p>
            </body>
        </html>
        """
    )
