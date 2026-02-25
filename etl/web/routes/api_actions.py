# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, Request


def build_api_actions_router(
    *,
    payload_with_pipeline: Callable[[Optional[dict[str, Any]], str], dict[str, Any]],
    action_validate: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    action_run: Callable[[Request, Optional[dict[str, Any]]], dict[str, Any]],
    stop_run: Callable[[str, Request, Optional[dict[str, Any]]], dict[str, Any]],
    resume_run: Callable[[str, Request, Optional[dict[str, Any]]], dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.post("/api/actions/validate")
    def api_action_validate(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return action_validate(request, payload)

    @router.post("/api/actions/run")
    def api_action_run(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return action_run(request, payload)

    @router.post("/api/pipelines/{pipeline_id:path}/validate")
    def api_pipeline_validate(
        request: Request,
        pipeline_id: str,
        payload: Optional[dict[str, Any]] = Body(default=None),
    ) -> dict:
        return action_validate(request, payload_with_pipeline(payload, pipeline_id))

    @router.post("/api/pipelines/{pipeline_id:path}/run")
    def api_pipeline_run(
        request: Request,
        pipeline_id: str,
        payload: Optional[dict[str, Any]] = Body(default=None),
    ) -> dict:
        return action_run(request, payload_with_pipeline(payload, pipeline_id))

    @router.post("/api/runs/{run_id}/stop")
    def api_stop_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return stop_run(run_id, request, payload)

    @router.post("/api/runs/{run_id}/resume")
    def api_resume_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return resume_run(run_id, request, payload)

    return router

