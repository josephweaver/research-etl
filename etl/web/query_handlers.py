# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from fastapi import HTTPException, Request


def _query_error_payload(error_code: str, message: str, detail: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"error_code": str(error_code or "execution_error"), "message": str(message or "")}
    if isinstance(detail, dict) and detail:
        payload["detail"] = detail
    return payload


def _resolve_query_executor(payload: dict[str, Any], deps: dict[str, Any]) -> tuple[str, Optional[Path], Optional[str]]:
    environments_config_raw = str(payload.get("environments_config") or "").strip()
    environments_config_path = Path(environments_config_raw).expanduser() if environments_config_raw else None
    env_name = str(payload.get("env") or "").strip() or None
    executor = str(payload.get("executor") or "").strip().lower()

    if env_name:
        try:
            resolved_env_cfg = deps["resolve_execution_config_path"](environments_config_path)
        except deps["ExecutionConfigError"] as exc:
            raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
        if resolved_env_cfg:
            try:
                envs = deps["load_execution_config"](resolved_env_cfg)
            except deps["ExecutionConfigError"] as exc:
                raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
            env_spec = envs.get(env_name)
            if isinstance(env_spec, dict):
                env_executor = str(env_spec.get("executor") or "").strip().lower()
                if env_executor:
                    executor = env_executor

    if not executor:
        executor = "local"
    if executor not in {"local", "slurm", "hpcc_direct"}:
        raise HTTPException(status_code=400, detail="`executor` must be one of: local, slurm, hpcc_direct.")
    return executor, environments_config_path, env_name


def _query_executor_instance(*, executor_name: str, execution_env: dict[str, Any], deps: dict[str, Any]):
    repo_root = Path(".").resolve()
    if executor_name == "slurm":
        return deps["SlurmExecutor"](env_config=execution_env, repo_root=repo_root, dry_run=True)
    if executor_name == "hpcc_direct":
        return deps["HpccDirectExecutor"](env_config=execution_env, repo_root=repo_root, dry_run=True)
    return deps["LocalExecutor"]()


def api_query_preview(request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    data = dict(payload or {})
    query_spec = data.get("query_spec")
    if not isinstance(query_spec, dict):
        raise HTTPException(
            status_code=400,
            detail=_query_error_payload("planner_error", "`query_spec` is required and must be an object."),
        )

    requested_project_id = deps["normalize_project_id"](str(data.get("project_id") or "").strip() or None)
    deps["require_project_access"](scope, requested_project_id)

    executor_name, environments_config_path, env_name = _resolve_query_executor(data, deps)
    global_config_raw = str(data.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    global_vars = deps["resolve_global_vars"](global_config_path)
    execution_env, _resolved_env_cfg, _selected_env_name = deps["resolve_execution_env"](
        environments_config_path,
        env_name,
        executor=executor_name,
        global_vars=global_vars,
    )

    ex = _query_executor_instance(executor_name=executor_name, execution_env=execution_env, deps=deps)
    caps = dict(getattr(ex, "capabilities", lambda: {})() or {})
    if not bool(caps.get("query_data")):
        raise HTTPException(
            status_code=400,
            detail=_query_error_payload(
                "transport_error",
                f"Executor '{executor_name}' does not support query_data.",
                detail={"executor": executor_name},
            ),
        )

    query_context = dict(data.get("query_context") or {})
    query_context.setdefault("project_id", requested_project_id)
    query_context.setdefault("executor", executor_name)
    query_context.setdefault("env", env_name)
    query_context.setdefault("repo_root", str(Path(".").resolve()))

    try:
        return ex.query_data(query_spec, context=query_context)
    except deps["QueryError"] as exc:
        detail_payload = exc.to_payload() if hasattr(exc, "to_payload") else _query_error_payload("execution_error", str(exc))
        raise HTTPException(status_code=int(getattr(exc, "http_status", 500) or 500), detail=detail_payload) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail=_query_error_payload("execution_error", f"Query preview failed: {exc}"),
        ) from exc


def api_query_schema(request: Request, payload: Optional[dict[str, Any]], deps: dict[str, Any]) -> dict:
    scope = deps["resolve_user_scope"](request)
    data = dict(payload or {})
    source = data.get("source")
    if not isinstance(source, (str, dict)):
        raise HTTPException(
            status_code=400,
            detail=_query_error_payload("planner_error", "`source` is required and must be string or object."),
        )

    requested_project_id = deps["normalize_project_id"](str(data.get("project_id") or "").strip() or None)
    deps["require_project_access"](scope, requested_project_id)

    executor_name, environments_config_path, env_name = _resolve_query_executor(data, deps)
    global_config_raw = str(data.get("global_config") or "").strip()
    global_config_path = Path(global_config_raw).expanduser() if global_config_raw else None
    global_vars = deps["resolve_global_vars"](global_config_path)
    execution_env, _resolved_env_cfg, _selected_env_name = deps["resolve_execution_env"](
        environments_config_path,
        env_name,
        executor=executor_name,
        global_vars=global_vars,
    )

    ex = _query_executor_instance(executor_name=executor_name, execution_env=execution_env, deps=deps)
    caps = dict(getattr(ex, "capabilities", lambda: {})() or {})
    if not bool(caps.get("query_data")):
        raise HTTPException(
            status_code=400,
            detail=_query_error_payload(
                "transport_error",
                f"Executor '{executor_name}' does not support query_data.",
                detail={"executor": executor_name},
            ),
        )

    query_context = dict(data.get("query_context") or {})
    query_context.setdefault("project_id", requested_project_id)
    query_context.setdefault("executor", executor_name)
    query_context.setdefault("env", env_name)
    query_context.setdefault("repo_root", str(Path(".").resolve()))

    query_spec = {
        "source": source,
        "select": ["*"],
        "limit": int(data.get("sample_limit") or 20),
        "offset": 0,
    }
    try:
        result = ex.query_data(query_spec, context=query_context)
        return {
            "columns": list(result.get("columns") or []),
            "rows": list(result.get("rows") or []),
            "row_count_estimate": result.get("row_count_estimate"),
            "executor": result.get("executor") or executor_name,
            "engine": result.get("engine") or "duckdb",
            "source": source,
        }
    except deps["QueryError"] as exc:
        detail_payload = exc.to_payload() if hasattr(exc, "to_payload") else _query_error_payload("execution_error", str(exc))
        raise HTTPException(status_code=int(getattr(exc, "http_status", 500) or 500), detail=detail_payload) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500,
            detail=_query_error_payload("execution_error", f"Query schema failed: {exc}"),
        ) from exc
