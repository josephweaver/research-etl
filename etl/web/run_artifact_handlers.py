# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request


def resolve_run_header(run_id: str, *, request: Request | None, deps: dict[str, Any]) -> dict:
    try:
        hdr = deps["fetch_run_header"](run_id)
    except deps["WebQueryError"] as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if hdr is None:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    if request is not None:
        scope = deps["resolve_user_scope"](request)
        deps["require_project_access"](scope, hdr.get("project_id"))
    return hdr


def artifact_executor_for(hdr: dict, deps: dict[str, Any]):
    executor_name = str(hdr.get("executor") or "local").strip().lower()
    if executor_name == "slurm":
        return deps["SlurmExecutor"](env_config={}, repo_root=Path(".").resolve(), dry_run=True)
    return deps["LocalExecutor"]()


def resolve_artifact_dir(hdr: dict) -> str:
    raw = (hdr.get("artifact_dir") or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="Run has no artifact_dir recorded.")
    return raw


def api_run_files(run_id: str, request: Request, deps: dict[str, Any]) -> dict:
    hdr = resolve_run_header(run_id, request=request, deps=deps)
    artifact_dir = resolve_artifact_dir(hdr)
    ex = artifact_executor_for(hdr, deps)
    try:
        return ex.artifact_tree(artifact_dir)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to build file tree: {exc}") from exc


def api_run_file(run_id: str, request: Request, path: str, deps: dict[str, Any]) -> dict:
    hdr = resolve_run_header(run_id, request=request, deps=deps)
    artifact_dir = resolve_artifact_dir(hdr)
    ex = artifact_executor_for(hdr, deps)
    try:
        return ex.artifact_file(artifact_dir, path, max_bytes=deps["MAX_FILE_VIEW_BYTES"])
    except Exception as exc:  # noqa: BLE001
        detail = str(exc)
        if "not found" in detail.lower():
            raise HTTPException(status_code=404, detail=detail) from exc
        if "invalid" in detail.lower():
            raise HTTPException(status_code=400, detail=detail) from exc
        raise HTTPException(status_code=500, detail=f"Failed to read file: {exc}") from exc


def api_run_live_log(run_id: str, request: Request, limit: int, deps: dict[str, Any]) -> dict:
    hdr = None
    try:
        hdr = deps["fetch_run_header"](run_id)
    except deps["WebQueryError"]:
        hdr = None

    scope = deps["resolve_user_scope"](request)
    if hdr is not None:
        deps["require_project_access"](scope, hdr.get("project_id"))
    else:
        with deps["LOCAL_RUN_LOCK"]:
            snap = dict(deps["LOCAL_RUN_SNAPSHOT"].get(run_id) or {})
        if not snap:
            raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
        deps["require_project_access"](scope, snap.get("project_id"))

    with deps["LOCAL_RUN_LOCK"]:
        snap = dict(deps["LOCAL_RUN_SNAPSHOT"].get(run_id) or {})
        ring_lines = list(deps["LOCAL_RUN_LOG_RING"].get(run_id) or [])
        state = str(snap.get("state") or "")
        log_file = str(snap.get("log_file") or "").strip()
    lines = ring_lines[-limit:] if ring_lines else []
    if log_file:
        file_lines = deps["tail_text_lines"](Path(log_file), limit=limit)
        if file_lines:
            lines = file_lines

    return {
        "run_id": run_id,
        "state": state,
        "log_file": log_file or None,
        "lines": lines,
    }
