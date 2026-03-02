# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, HTTPException, Query, Request


def build_api_read_router(
    *,
    resolve_user_scope: Callable[[Request], Any],
    resolve_request_project_filter: Callable[..., tuple[Any, Optional[str]]],
    require_project_access: Callable[[Any, Optional[str]], Optional[str]],
    combine_project_scoped_rows: Callable[..., list[dict[str, Any]]],
    parse_dataset_create_payload: Callable[[Optional[dict[str, Any]]], dict[str, Any]],
    fetch_pipelines: Callable[..., list[dict[str, Any]]],
    fetch_pipeline_runs: Callable[..., list[dict[str, Any]]],
    fetch_pipeline_validations: Callable[..., list[dict[str, Any]]],
    fetch_pipeline_detail: Callable[..., Optional[dict[str, Any]]],
    fetch_datasets: Callable[..., list[dict[str, Any]]],
    fetch_dataset_detail: Callable[..., Optional[dict[str, Any]]],
    fetch_runs: Callable[..., list[dict[str, Any]]],
    fetch_run_detail: Callable[..., Optional[dict[str, Any]]],
    fetch_executor_capabilities: Callable[[], list[dict[str, Any]]],
    create_dataset: Callable[..., dict[str, Any]],
    WebQueryError: type[Exception],
    DatasetServiceError: type[Exception],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/executors/capabilities")
    def api_executor_capabilities(request: Request) -> list[dict[str, Any]]:
        _ = resolve_user_scope(request)
        return fetch_executor_capabilities()

    @router.get("/api/pipelines")
    def api_pipelines(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        q: Optional[str] = Query(default=None),
        project_id: Optional[str] = Query(default=None),
    ) -> list[dict]:
        scope, selected_project = resolve_request_project_filter(request=request, project_id=project_id)
        try:
            if selected_project:
                return fetch_pipelines(limit=limit, q=q, project_id=selected_project)
            rows_by_project = [
                fetch_pipelines(limit=limit, q=q, project_id=pid)
                for pid in sorted(scope.allowed_projects)
            ]
            return combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="pipeline")
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/api/pipelines/{pipeline_id:path}/runs")
    def api_pipeline_runs(
        request: Request,
        pipeline_id: str,
        limit: int = Query(default=50, ge=1, le=500),
        status: Optional[str] = Query(default=None),
        executor: Optional[str] = Query(default=None),
        project_id: Optional[str] = Query(default=None),
    ) -> list[dict]:
        scope, selected_project = resolve_request_project_filter(
            request=request,
            project_id=project_id,
            pipeline_id=pipeline_id,
        )
        try:
            if selected_project:
                return fetch_pipeline_runs(
                    pipeline_id,
                    limit=limit,
                    status=status,
                    executor=executor,
                    project_id=selected_project,
                )
            rows_by_project = [
                fetch_pipeline_runs(
                    pipeline_id,
                    limit=limit,
                    status=status,
                    executor=executor,
                    project_id=pid,
                )
                for pid in sorted(scope.allowed_projects)
            ]
            return combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="run_id")
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/api/pipelines/{pipeline_id:path}/validations")
    def api_pipeline_validations(
        request: Request,
        pipeline_id: str,
        limit: int = Query(default=50, ge=1, le=500),
        project_id: Optional[str] = Query(default=None),
    ) -> list[dict]:
        scope, selected_project = resolve_request_project_filter(
            request=request,
            project_id=project_id,
            pipeline_id=pipeline_id,
        )
        try:
            if selected_project:
                return fetch_pipeline_validations(pipeline_id, limit=limit, project_id=selected_project)
            rows_by_project = [
                fetch_pipeline_validations(pipeline_id, limit=limit, project_id=pid)
                for pid in sorted(scope.allowed_projects)
            ]
            return combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="validation_id")
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/api/pipelines/{pipeline_id:path}")
    def api_pipeline_detail(request: Request, pipeline_id: str, project_id: Optional[str] = Query(default=None)) -> dict:
        scope, selected_project = resolve_request_project_filter(
            request=request,
            project_id=project_id,
            pipeline_id=pipeline_id,
        )
        try:
            payload = None
            if selected_project:
                payload = fetch_pipeline_detail(pipeline_id, project_id=selected_project)
            else:
                for pid in sorted(scope.allowed_projects):
                    payload = fetch_pipeline_detail(pipeline_id, project_id=pid)
                    if payload is not None:
                        break
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_id}")
        return payload

    @router.get("/api/datasets")
    def api_datasets(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        q: Optional[str] = Query(default=None),
    ) -> list[dict]:
        _ = resolve_user_scope(request)
        try:
            return fetch_datasets(limit=limit, q=q)
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.post("/api/datasets")
    def api_create_dataset(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        _ = resolve_user_scope(request)
        args = parse_dataset_create_payload(payload)
        try:
            return create_dataset(**args)
        except DatasetServiceError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/api/datasets/{dataset_id:path}")
    def api_dataset_detail(request: Request, dataset_id: str) -> dict:
        _ = resolve_user_scope(request)
        try:
            payload = fetch_dataset_detail(dataset_id)
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_id}")
        return payload

    @router.get("/api/runs")
    def api_runs(
        request: Request,
        limit: int = Query(default=50, ge=1, le=500),
        status: Optional[str] = Query(default=None),
        executor: Optional[str] = Query(default=None),
        q: Optional[str] = Query(default=None),
        project_id: Optional[str] = Query(default=None),
    ) -> list[dict]:
        scope, selected_project = resolve_request_project_filter(request=request, project_id=project_id)
        try:
            if selected_project:
                return fetch_runs(
                    limit=limit,
                    status=status,
                    executor=executor,
                    q=q,
                    project_id=selected_project,
                )
            rows_by_project = [
                fetch_runs(
                    limit=limit,
                    status=status,
                    executor=executor,
                    q=q,
                    project_id=pid,
                )
                for pid in sorted(scope.allowed_projects)
            ]
            return combine_project_scoped_rows(rows_by_project, limit=limit, dedupe_key="run_id")
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @router.get("/api/runs/{run_id}/live")
    def api_run_live(run_id: str, request: Request) -> dict:
        scope = resolve_user_scope(request)
        try:
            payload = fetch_run_detail(run_id)
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        require_project_access(scope, payload.get("project_id"))

        attempts = payload.get("attempts") or []
        events = payload.get("events") or []
        steps = payload.get("steps") or []
        active_attempts = [
            a
            for a in attempts
            if not bool(a.get("skipped")) and not bool(a.get("success")) and not a.get("ended_at")
        ]
        failed_steps = [s for s in steps if not bool(s.get("success")) and not bool(s.get("skipped"))]
        completed_steps = [s for s in steps if bool(s.get("success")) and not bool(s.get("skipped"))]
        skipped_steps = [s for s in steps if bool(s.get("skipped"))]

        return {
            "run_id": payload.get("run_id"),
            "pipeline": payload.get("pipeline"),
            "project_id": payload.get("project_id"),
            "status": payload.get("status"),
            "success": bool(payload.get("success")),
            "executor": payload.get("executor"),
            "started_at": payload.get("started_at"),
            "ended_at": payload.get("ended_at"),
            "latest_event": events[-1] if events else None,
            "events": events,
            "active_attempt_count": len(active_attempts),
            "active_attempts": active_attempts,
            "completed_step_count": len(completed_steps),
            "failed_step_count": len(failed_steps),
            "skipped_step_count": len(skipped_steps),
            "provenance": payload.get("provenance") or {},
        }

    @router.get("/api/runs/{run_id}")
    def api_run_detail(run_id: str, request: Request) -> dict:
        scope = resolve_user_scope(request)
        try:
            payload = fetch_run_detail(run_id)
        except WebQueryError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        if payload is None:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        require_project_access(scope, payload.get("project_id"))
        return payload

    return router
