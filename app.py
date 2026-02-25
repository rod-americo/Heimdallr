# Copyright (c) 2026 Rodrigo Americo
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Heimdallr Unified FastAPI Server
# Combines upload ingestion with web dashboard and RESTful API
# Port: 8001

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import config
from api.routes import patients, proxy, upload

# Initialize FastAPI application
app = FastAPI(title=config.SERVER_TITLE)

# Ensure required directories exist
config.ensure_directories()

# ============================================================
# INCLUDE ROUTERS
# ============================================================
app.include_router(patients.router)
app.include_router(proxy.router)
app.include_router(upload.router)

@app.get("/api/tools/uploader")
async def download_uploader():
    """
    Download the CLI uploader script for convenient exam submission.
    Allows users to download uploader.py directly from the web UI.
    """
    script_path = config.BASE_DIR / "clients" / "uploader.py"
    return FileResponse(
        path=script_path,
        filename="uploader.py",
        media_type="text/x-python"
    )

# ============================================================
# STATIC FILES & WEB DASHBOARD
# ============================================================

# Mount static assets (CSS, JS, images) if directory exists
if config.STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=config.STATIC_DIR), name="static")

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """
    Serve the web dashboard (main entry point).
    """
    index_path = config.STATIC_DIR / "index.html"
    if index_path.exists():
        with open(index_path, 'r') as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(content="""
        <html>
            <head><title>Heimdallr</title></head>
            <body style="font-family: sans-serif; padding: 40px; background: #1a1a2e; color: #eee;">
                <h1>🔭 Heimdallr</h1>
                <p>Dashboard not found. Please create <code>static/index.html</code></p>
                <p><a href="/docs" style="color: #4cc9f0;">API Documentation →</a></p>
            </body>
        </html>
        """)

# Entry point when running directly (python app.py)
if __name__ == "__main__":
    import uvicorn
    # Run server on all interfaces, port 8001
    uvicorn.run("app:app", host="0.0.0.0", port=8001, reload=True)
