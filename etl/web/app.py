# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .routes.ui import router as ui_router

WEB_ASSETS_DIR = Path(__file__).resolve().parents[2] / "web" / "assets"


def register_ui(app: FastAPI) -> None:
    app.mount("/web/assets", StaticFiles(directory=str(WEB_ASSETS_DIR)), name="web-assets")
    app.include_router(ui_router)

