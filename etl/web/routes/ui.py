# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, HTMLResponse

router = APIRouter()
WEB_PAGES_DIR = Path(__file__).resolve().parents[3] / "web" / "pages"
WEB_ASSETS_DIR = Path(__file__).resolve().parents[3] / "web" / "assets"


@lru_cache(maxsize=32)
def _load_web_page(page_name: str) -> str:
    page_key = str(page_name or "").strip()
    if not page_key:
        raise RuntimeError("Empty web page key")
    page_path = (WEB_PAGES_DIR / page_key).resolve()
    try:
        page_path.relative_to(WEB_PAGES_DIR.resolve())
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Invalid web page path: {page_key}") from exc
    if not page_path.exists():
        raise RuntimeError(f"Web page not found: {page_key}")
    return page_path.read_text(encoding="utf-8")


def _render_web_page(page_name: str) -> HTMLResponse:
    try:
        return HTMLResponse(_load_web_page(page_name))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to render web page '{page_name}': {exc}") from exc


@router.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return _render_web_page("index.html")


@router.get("/favicon.ico")
def favicon() -> FileResponse:
    icon = (WEB_ASSETS_DIR / "favicon.svg").resolve()
    if not icon.exists():
        raise HTTPException(status_code=404, detail="Favicon not found.")
    return FileResponse(str(icon), media_type="image/svg+xml")


@router.get("/pipelines", response_class=HTMLResponse)
def pipelines_index() -> HTMLResponse:
    return _render_web_page("pipelines.html")


@router.get("/plugins", response_class=HTMLResponse)
def plugins_index() -> HTMLResponse:
    return _render_web_page("plugins.html")


@router.get("/project-dag", response_class=HTMLResponse)
def project_dag_index() -> HTMLResponse:
    return _render_web_page("project-dag.html")


@router.get("/projects/{project_id}/dag", response_class=HTMLResponse)
def project_dag_project_index(project_id: str) -> HTMLResponse:
    _ = project_id
    return _render_web_page("projects-dag.html")


@router.get("/datasets", response_class=HTMLResponse)
def datasets_index() -> HTMLResponse:
    return _render_web_page("datasets.html")


@router.get("/datasets/{dataset_id:path}", response_class=HTMLResponse)
def dataset_detail_index(dataset_id: str) -> HTMLResponse:
    _ = dataset_id
    return _render_web_page("dataset-detail.html")


@router.get("/pipelines/new", response_class=HTMLResponse)
def pipelines_new_index() -> HTMLResponse:
    return _render_web_page("pipeline-new.html")


@router.get("/pipelines/{pipeline_id:path}/edit", response_class=HTMLResponse)
def pipeline_edit_index(pipeline_id: str) -> HTMLResponse:
    _ = pipeline_id
    return _render_web_page("pipeline-edit.html")


@router.get("/pipelines/{pipeline_id:path}", response_class=HTMLResponse)
def pipeline_detail_index(pipeline_id: str) -> HTMLResponse:
    _ = pipeline_id
    return _render_web_page("pipeline-detail.html")


@router.get("/runs/{run_id:path}/live", response_class=HTMLResponse)
def run_live_index(run_id: str) -> HTMLResponse:
    _ = run_id
    return _render_web_page("run-live.html")
