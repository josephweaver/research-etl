# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable

from fastapi import APIRouter, Query, Request


def build_api_run_artifacts_router(
    *,
    run_files: Callable[[str, Request], dict[str, Any]],
    run_file: Callable[[str, Request, str], dict[str, Any]],
    run_live_log: Callable[[str, Request, int], dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/runs/{run_id}/files")
    def api_run_files(run_id: str, request: Request) -> dict:
        return run_files(run_id, request)

    @router.get("/api/runs/{run_id}/file")
    def api_run_file(run_id: str, request: Request, path: str = Query(default="")) -> dict:
        return run_file(run_id, request, path)

    @router.get("/api/runs/{run_id}/live-log")
    def api_run_live_log(run_id: str, request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> dict:
        return run_live_log(run_id, request, limit)

    return router

