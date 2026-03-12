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
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/query/preview")
    def api_query_preview(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return query_preview(request, payload)

    @router.post("/api/query/schema")
    def api_query_schema(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return query_schema(request, payload)

    return router
