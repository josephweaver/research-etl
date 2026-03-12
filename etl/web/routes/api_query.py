# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, Request


def build_api_query_router(
    *,
    query_preview: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    query_schema: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    query_workspace: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    query_workspace_save: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/query/preview")
    def api_query_preview(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return query_preview(request, payload)

    @router.post("/api/query/schema")
    def api_query_schema(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return query_schema(request, payload)

    @router.get("/api/query/workspace")
    def api_query_workspace(
        request: Request,
        project_id: Optional[str] = None,
        projects_config: Optional[str] = None,
    ) -> dict:
        payload: dict[str, Any] = {}
        if project_id is not None:
            payload["project_id"] = project_id
        if projects_config is not None:
            payload["projects_config"] = projects_config
        return query_workspace(request, payload)

    @router.post("/api/query/workspace")
    def api_query_workspace_save(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return query_workspace_save(request, payload)

    return router
