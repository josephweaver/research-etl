# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import APIRouter, Body, Query, Request


def build_api_builder_router(
    *,
    builder_source: Callable[..., dict[str, Any]],
    builder_files: Callable[..., dict[str, Any]],
    builder_plugins: Callable[..., dict[str, Any]],
    builder_environments: Callable[..., dict[str, Any]],
    builder_projects: Callable[..., dict[str, Any]],
    builder_project_vars: Callable[..., dict[str, Any]],
    builder_git_status: Callable[..., dict[str, Any]],
    builder_git_main_check: Callable[..., dict[str, Any]],
    builder_git_sync: Callable[..., dict[str, Any]],
    builder_resolve_text: Callable[..., dict[str, Any]],
    builder_namespace: Callable[..., dict[str, Any]],
    builder_validate: Callable[..., dict[str, Any]],
    builder_generate: Callable[..., dict[str, Any]],
    builder_test_step: Callable[..., dict[str, Any]],
    builder_test_step_start: Callable[..., dict[str, Any]],
    builder_test_step_status: Callable[..., dict[str, Any]],
    builder_test_step_stop: Callable[..., dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/builder/source")
    def api_builder_source(
        pipeline: str = Query(default=""),
        project_id: Optional[str] = Query(default=None),
        projects_config: Optional[str] = Query(default=None),
        pipeline_source: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_source(
            pipeline=pipeline,
            project_id=project_id,
            projects_config=projects_config,
            pipeline_source=pipeline_source,
        )

    @router.get("/api/builder/files")
    def api_builder_files(
        project_id: Optional[str] = Query(default=None),
        projects_config: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_files(project_id=project_id, projects_config=projects_config)

    @router.get("/api/builder/plugins")
    def api_builder_plugins(
        global_config: Optional[str] = Query(default=None),
        plugins_dir: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_plugins(global_config=global_config, plugins_dir=plugins_dir)

    @router.get("/api/builder/environments")
    def api_builder_environments(
        environments_config: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_environments(environments_config=environments_config)

    @router.get("/api/builder/projects")
    def api_builder_projects(request: Request, projects_config: Optional[str] = Query(default=None)) -> dict:
        return builder_projects(request=request, projects_config=projects_config)

    @router.get("/api/builder/project-vars")
    def api_builder_project_vars(
        request: Request,
        project_id: Optional[str] = Query(default=None),
        projects_config: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_project_vars(request=request, project_id=project_id, projects_config=projects_config)

    @router.get("/api/builder/git-status")
    def api_builder_git_status(
        project_id: Optional[str] = Query(default=None),
        projects_config: Optional[str] = Query(default=None),
        pipeline_source: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_git_status(
            project_id=project_id,
            projects_config=projects_config,
            pipeline_source=pipeline_source,
        )

    @router.get("/api/builder/git-main-check")
    def api_builder_git_main_check(
        project_id: Optional[str] = Query(default=None),
        projects_config: Optional[str] = Query(default=None),
        pipeline_source: Optional[str] = Query(default=None),
    ) -> dict:
        return builder_git_main_check(
            project_id=project_id,
            projects_config=projects_config,
            pipeline_source=pipeline_source,
        )

    @router.post("/api/builder/git-sync")
    def api_builder_git_sync(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_git_sync(payload=payload)

    @router.post("/api/builder/resolve-text")
    def api_builder_resolve_text(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_resolve_text(payload=payload)

    @router.post("/api/builder/namespace")
    def api_builder_namespace(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_namespace(payload=payload)

    @router.post("/api/builder/validate")
    def api_builder_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_validate(payload=payload)

    @router.post("/api/builder/generate")
    def api_builder_generate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_generate(payload=payload)

    @router.post("/api/builder/test-step")
    def api_builder_test_step(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_test_step(payload=payload)

    @router.post("/api/builder/test-step/start")
    def api_builder_test_step_start(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_test_step_start(payload=payload)

    @router.get("/api/builder/test-step/status")
    def api_builder_test_step_status(test_id: str = Query(default="")) -> dict:
        return builder_test_step_status(test_id=test_id)

    @router.post("/api/builder/test-step/stop")
    def api_builder_test_step_stop(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
        return builder_test_step_stop(payload=payload)

    return router

