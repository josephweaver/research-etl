# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, Query, Request


def build_api_management_router(
    *,
    pipelines_create: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    pipelines_update: Callable[[Request, str, Optional[dict[str, Any]]], dict[str, Any]],
    project_dag: Callable[..., dict[str, Any]],
    plugins_stats: Callable[..., dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/pipelines")
    def api_pipelines_create(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return pipelines_create(request, payload)

    @router.put("/api/pipelines/{pipeline_id:path}")
    def api_pipelines_update(
        request: Request,
        pipeline_id: str,
        payload: Optional[dict[str, Any]] = Body(default=None),
    ) -> dict:
        return pipelines_update(request, pipeline_id, payload)

    @router.get("/api/projects/{project_id}/dag")
    def api_project_dag(
        request: Request,
        project_id: str,
        projects_config: Optional[str] = Query(default=None),
    ) -> dict:
        return project_dag(
            request=request,
            project_id=project_id,
            projects_config=projects_config,
        )

    @router.get("/api/plugins/stats")
    def api_plugins_stats(
        global_config: Optional[str] = Query(default=None),
        plugins_dir: Optional[str] = Query(default=None),
        environments_config: Optional[str] = Query(default=None),
        env: Optional[str] = Query(default=None),
        low_sample_multiplier: float = Query(default=1.5, ge=1.0, le=10.0),
        limit: int = Query(default=200, ge=10, le=5000),
    ) -> dict:
        return plugins_stats(
            global_config=global_config,
            plugins_dir=plugins_dir,
            environments_config=environments_config,
            env=env,
            low_sample_multiplier=low_sample_multiplier,
            limit=limit,
        )

    return router

