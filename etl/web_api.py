# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import os
import tempfile
import shutil
import json
import shlex
import re

import logging
import subprocess
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import psycopg
from fastapi import Body, FastAPI, HTTPException, Query, Request

from .config import ConfigError, load_global_config, resolve_global_config_path
from .ai_pipeline import AIPipelineError, generate_pipeline_draft
from .db import get_database_url
from .datasets import DatasetServiceError, create_dataset
from .execution_config import (
    ExecutionConfigError,
    apply_execution_env_overrides,
    load_execution_config,
    resolve_execution_env_templates,
    resolve_execution_config_path,
    validate_environment_executor,
)
from .executors.hpcc_direct import HpccDirectExecutor
from .executors.local import LocalExecutor
from .executors.slurm import SlurmExecutor
from .git_checkout import infer_repo_name
from .query.errors import QueryError
from .pipeline import (
    DEFAULT_RESOLVE_MAX_PASSES,
    Pipeline,
    Step,
    parse_pipeline,
    PipelineError,
    resolve_max_passes_setting,
)
from .pipeline_assets import (
    PipelineAssetError,
    pipeline_asset_sources_from_project_vars,
    resolve_pipeline_path_from_project_sources,
    sync_pipeline_asset_source,
)
from .provenance import collect_run_provenance
from .projects import (
    ProjectConfigError,
    infer_project_id_from_pipeline_path,
    load_project_vars,
    normalize_project_id,
    resolve_project_id,
    resolve_projects_config_path,
)
from .plugins.base import PluginLoadError, load_plugin
from .runner import run_pipeline
from .runtime_context import RuntimeContext, RuntimeContextError, RuntimeContextRequest, build_runtime_context
from .tracking import (
    fetch_plugin_resource_stats,
    upsert_run_status,
    upsert_run_context_snapshot,
    load_latest_run_context_snapshot,
    load_runs,
)
from .variable_solver import VariableSolver
from .web import action_handlers as web_action_handlers
from .web.app import register_ui
from .web import builder_handlers as web_builder_handlers
from .web import query_handlers as web_query_handlers
from .web import run_artifact_handlers as web_run_artifact_handlers
from .web.helpers import (
    extract_unresolved_tokens as _extract_unresolved_tokens,
    parse_bool as _parse_bool,
    resolve_workdir_from_solver as _resolve_workdir_from_solver,
)
from .web.routes.api_actions import build_api_actions_router
from .web.routes.api_builder import build_api_builder_router
from .web.routes.api_management import build_api_management_router
from .web.routes.api_query import build_api_query_router
from .web.routes.api_read import build_api_read_router
from .web.routes.api_run_artifacts import build_api_run_artifacts_router
from .web_queries import (
    WebQueryError,
    fetch_dataset_detail,
    fetch_datasets,
    fetch_pipeline_detail,
    fetch_pipeline_runs,
    fetch_pipeline_validations,
    fetch_pipelines,
    fetch_run_detail,
    fetch_run_header,
    fetch_runs,
)
from .subprocess_logging import run_logged_subprocess


app = FastAPI(title="Research ETL UI", version="0.1.0")
_LOG = logging.getLogger("etl.web_api")
register_ui(app)

MAX_FILE_VIEW_BYTES = 256 * 1024
_TPL_RE = re.compile(r"\{([^{}]+)\}")
_LOCAL_RUN_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="etl-web-local")
_LOCAL_RUN_LOCK = threading.Lock()
_ACTIVE_LOCAL_RUN_KEYS: dict[str, str] = {}
_LOCAL_RUN_SNAPSHOT: dict[str, dict[str, Any]] = {}
_LOCAL_RUN_KEY_BY_RUN_ID: dict[str, str] = {}
_LOCAL_RUN_FUTURES: dict[str, Any] = {}
_LOCAL_RUN_CANCEL_REQUESTED: set[str] = set()
_LOCAL_RUN_LOG_RING: dict[str, list[str]] = {}
_LOCAL_RUN_LOG_RING_MAX = 2000
_WEB_RUNTIME_CONTEXT: Optional[RuntimeContext] = None
_BUILDER_SESSIONS_LOCK = threading.Lock()


@app.on_event("startup")
def _init_web_runtime_context() -> None:
    global _WEB_RUNTIME_CONTEXT
    try:
        _WEB_RUNTIME_CONTEXT = build_runtime_context(
            RuntimeContextRequest(
                bootstrap_label="web",
                logger_name="etl.web_api",
            )
        )
        _LOG.info(
            "Web bootstrap runtime context initialized log=%s",
            _WEB_RUNTIME_CONTEXT.logging.bootstrap_log_file.as_posix(),
        )
    except RuntimeContextError as exc:
        _LOG.warning("Web bootstrap runtime context initialization failed: %s", exc)
        _WEB_RUNTIME_CONTEXT = None


@dataclass(frozen=True)
class UserScope:
    user_id: str
    allowed_projects: set[str]


def _local_submission_key(
    *,
    pipeline_path: Path,
    project_id: Optional[str],
    env_name: Optional[str],
    execution_source: Optional[str],
) -> str:
    return "||".join(
        [
            str(pipeline_path.resolve()).lower(),
            str(project_id or "").lower(),
            str(env_name or "").lower(),
            str(execution_source or "").lower(),
            "local",
        ]
    )


def _set_local_run_snapshot(run_id: str, **updates: Any) -> None:
    with _LOCAL_RUN_LOCK:
        base = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        base.update(updates)
        _LOCAL_RUN_SNAPSHOT[run_id] = base


def _append_local_run_log(run_id: str, message: str, level: str = "INFO") -> None:
    ts = datetime.utcnow().isoformat() + "Z"
    line = f"[{ts}] [{str(level or 'INFO').upper()}] {str(message or '').rstrip()}"
    with _LOCAL_RUN_LOCK:
        ring = _LOCAL_RUN_LOG_RING.setdefault(run_id, [])
        ring.append(line)
        if len(ring) > _LOCAL_RUN_LOG_RING_MAX:
            del ring[: len(ring) - _LOCAL_RUN_LOG_RING_MAX]
        snap = dict(_LOCAL_RUN_SNAPSHOT.get(run_id) or {})
        log_file = str(snap.get("log_file") or "").strip()
    if log_file:
        try:
            p = Path(log_file)
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("a", encoding="utf-8", errors="replace") as f:
                f.write(line + "\n")
        except Exception:
            pass


def _tail_text_lines(path: Path, limit: int = 200) -> list[str]:
    if not path.exists() or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []
    lines = text.splitlines()
    if limit <= 0:
        return lines
    return lines[-limit:]


def _last_non_empty_line(lines: list[str]) -> str:
    for line in reversed(list(lines or [])):
        text = str(line or "").strip()
        if text:
            return text
    return ""


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _builder_sessions_root() -> Path:
    root = (Path(".") / ".runs" / "builder" / "sessions").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _normalize_builder_session_id(raw: Optional[str]) -> str:
    text = str(raw or "").strip()
    if not text:
        return uuid.uuid4().hex
    text = re.sub(r"[^A-Za-z0-9_.-]+", "-", text)
    text = text.strip(".-")
    return text or uuid.uuid4().hex


def _builder_session_dir(session_id: str) -> Path:
    safe_id = _normalize_builder_session_id(session_id)
    return (_builder_sessions_root() / safe_id).resolve()


def _builder_session_file(session_id: str) -> Path:
    return _builder_session_dir(session_id) / "session.json"


def _builder_session_context_file(session_id: str) -> Path:
    return _builder_session_dir(session_id) / "context.json"


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists() or not path.is_file():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _db_session_row_to_obj(row: Any) -> dict[str, Any]:
    return {
        "session_id": str(row[0] or ""),
        "run_id": str(row[1] or ""),
        "run_started_at": row[2].isoformat().replace("+00:00", "Z") if row[2] is not None else "",
        "pipeline": str(row[3] or ""),
        "project_id": str(row[4] or ""),
        "env": str(row[5] or ""),
        "executor": str(row[6] or ""),
        "status": str(row[7] or ""),
        "context_file": str(row[8] or ""),
        "last_step_name": str(row[9] or ""),
        "last_step_index": row[10],
        "last_result": str(row[11] or ""),
        "last_error": str(row[12] or ""),
        "last_updated_at": row[13].isoformat().replace("+00:00", "Z") if row[13] is not None else "",
        "created_at": row[14].isoformat().replace("+00:00", "Z") if row[14] is not None else "",
        "updated_at": row[15].isoformat().replace("+00:00", "Z") if row[15] is not None else "",
    }


def _db_load_builder_session(session_id: str) -> Optional[dict[str, Any]]:
    db_url = get_database_url()
    if not db_url:
        return None
    sid = _normalize_builder_session_id(session_id)
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        session_id, run_id, run_started_at, pipeline, project_id,
                        env_name, executor, status, context_file, last_step_name,
                        last_step_index, last_result, last_error, last_updated_at,
                        created_at, updated_at
                    FROM etl_builder_sessions
                    WHERE session_id = %s
                    """,
                    (sid,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return _db_session_row_to_obj(row)
    except Exception:
        return None


def _db_upsert_builder_session(session: dict[str, Any]) -> Optional[dict[str, Any]]:
    db_url = get_database_url()
    if not db_url:
        return None
    sid = _normalize_builder_session_id(str(session.get("session_id") or ""))
    if not sid:
        return None
    run_id = str(session.get("run_id") or "").strip() or uuid.uuid4().hex
    run_started_at = str(session.get("run_started_at") or "").strip() or _utc_now_iso()
    context_file = str(session.get("context_file") or "").strip() or _builder_session_context_file(sid).as_posix()
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_builder_sessions (
                        session_id, run_id, run_started_at, pipeline, project_id, env_name, executor,
                        status, context_file, last_step_name, last_step_index, last_result, last_error,
                        last_updated_at, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                    ON CONFLICT (session_id)
                    DO UPDATE SET
                        run_id = EXCLUDED.run_id,
                        run_started_at = COALESCE(EXCLUDED.run_started_at, etl_builder_sessions.run_started_at),
                        pipeline = EXCLUDED.pipeline,
                        project_id = EXCLUDED.project_id,
                        env_name = EXCLUDED.env_name,
                        executor = EXCLUDED.executor,
                        status = EXCLUDED.status,
                        context_file = EXCLUDED.context_file,
                        last_step_name = EXCLUDED.last_step_name,
                        last_step_index = EXCLUDED.last_step_index,
                        last_result = EXCLUDED.last_result,
                        last_error = EXCLUDED.last_error,
                        last_updated_at = EXCLUDED.last_updated_at,
                        updated_at = NOW()
                    RETURNING
                        session_id, run_id, run_started_at, pipeline, project_id,
                        env_name, executor, status, context_file, last_step_name,
                        last_step_index, last_result, last_error, last_updated_at,
                        created_at, updated_at
                    """,
                    (
                        sid,
                        run_id,
                        run_started_at,
                        str(session.get("pipeline") or "").strip() or None,
                        normalize_project_id(session.get("project_id")) if session.get("project_id") else None,
                        str(session.get("env") or "").strip() or None,
                        str(session.get("executor") or "").strip() or None,
                        str(session.get("status") or "active"),
                        context_file,
                        str(session.get("last_step_name") or "").strip() or None,
                        session.get("last_step_index"),
                        str(session.get("last_result") or "").strip() or None,
                        str(session.get("last_error") or "").strip() or None,
                        str(session.get("last_updated_at") or "").strip() or None,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
        if not row:
            return None
        return _db_session_row_to_obj(row)
    except Exception:
        return None


def _db_insert_builder_session_step(*, session_id: str, result: dict[str, Any]) -> None:
    db_url = get_database_url()
    if not db_url:
        return
    sid = _normalize_builder_session_id(session_id)
    if not sid:
        return
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_builder_session_steps (
                        session_id, run_id, step_name, step_index, success, error
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        sid,
                        str(result.get("run_id") or "").strip() or None,
                        str(result.get("step_name") or "").strip() or None,
                        result.get("step_index"),
                        bool(result.get("success")),
                        str(result.get("error") or "").strip() or None,
                    ),
                )
            conn.commit()
    except Exception:
        return


def _db_list_builder_sessions(*, pipeline: Optional[str], project_id: Optional[str], env: Optional[str]) -> Optional[list[dict[str, Any]]]:
    db_url = get_database_url()
    if not db_url:
        return None
    where_parts: list[str] = []
    params: list[Any] = []
    if pipeline:
        where_parts.append("pipeline = %s")
        params.append(str(pipeline).strip())
    if project_id:
        where_parts.append("project_id = %s")
        params.append(normalize_project_id(project_id))
    if env:
        where_parts.append("env_name = %s")
        params.append(str(env).strip())
    where_sql = (" WHERE " + " AND ".join(where_parts)) if where_parts else ""
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        session_id, run_id, run_started_at, pipeline, project_id,
                        env_name, executor, status, context_file, last_step_name,
                        last_step_index, last_result, last_error, last_updated_at,
                        created_at, updated_at
                    FROM etl_builder_sessions
                    {where_sql}
                    ORDER BY updated_at DESC, created_at DESC, session_id DESC
                    """,
                    tuple(params),
                )
                rows = cur.fetchall() or []
        return [_db_session_row_to_obj(r) for r in rows]
    except Exception:
        return None


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def _load_builder_session(session_id: str) -> Optional[dict[str, Any]]:
    sid = _normalize_builder_session_id(session_id)
    db_data = _db_load_builder_session(sid)
    if isinstance(db_data, dict):
        if not str(db_data.get("context_file") or "").strip():
            db_data["context_file"] = _builder_session_context_file(sid).as_posix()
        return db_data
    data = _read_json_file(_builder_session_file(sid), default=None)
    if not isinstance(data, dict):
        return None
    data["session_id"] = sid
    if not str(data.get("context_file") or "").strip():
        data["context_file"] = _builder_session_context_file(sid).as_posix()
    return data


def _save_builder_session(session: dict[str, Any]) -> dict[str, Any]:
    sid = _normalize_builder_session_id(str(session.get("session_id") or ""))
    now = _utc_now_iso()
    cur = dict(_load_builder_session(sid) or {})
    out = dict(cur)
    out.update(dict(session or {}))
    out["session_id"] = sid
    out["created_at"] = str(out.get("created_at") or cur.get("created_at") or now)
    out["updated_at"] = now
    if not str(out.get("run_id") or "").strip():
        out["run_id"] = uuid.uuid4().hex
    if not str(out.get("run_started_at") or "").strip():
        out["run_started_at"] = now
    if not str(out.get("context_file") or "").strip():
        out["context_file"] = _builder_session_context_file(sid).as_posix()
    db_out = _db_upsert_builder_session(out)
    if isinstance(db_out, dict):
        out = dict(db_out)
    with _BUILDER_SESSIONS_LOCK:
        _write_json_atomic(_builder_session_file(sid), out)
    return out


def _builder_step_session_prepare(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    data = dict(payload or {})
    requested_sid = str(data.get("session_id") or "").strip()
    run_id = str(data.get("run_id") or "").strip()
    sid = _normalize_builder_session_id(requested_sid or run_id or uuid.uuid4().hex)
    current = _load_builder_session(sid) or {}
    now = _utc_now_iso()
    session = _save_builder_session(
        {
            "session_id": sid,
            "run_id": run_id or str(current.get("run_id") or "").strip() or uuid.uuid4().hex,
            "run_started_at": str(data.get("run_started_at") or "").strip() or str(current.get("run_started_at") or now),
            "pipeline": str(data.get("pipeline") or "").strip() or str(current.get("pipeline") or "").strip(),
            "project_id": str(data.get("project_id") or "").strip() or str(current.get("project_id") or "").strip(),
            "env": str(data.get("env") or "").strip() or str(current.get("env") or "").strip(),
            "executor": str(data.get("executor") or "").strip() or str(current.get("executor") or "").strip(),
            "status": str(current.get("status") or "active"),
        }
    )
    context_path = Path(str(session.get("context_file") or "").strip() or _builder_session_context_file(sid).as_posix())
    if not context_path.exists():
        _write_json_atomic(context_path, {})
    data["session_id"] = sid
    data["run_id"] = str(session.get("run_id") or "")
    data["run_started_at"] = str(session.get("run_started_at") or "")
    data["context_file"] = context_path.as_posix()
    return {"session": session, "payload": data}


def _builder_step_session_load_context(*, context_file: Optional[str]) -> dict[str, Any]:
    path = Path(str(context_file or "").strip()) if str(context_file or "").strip() else Path()
    data = _read_json_file(path, default={}) if path else {}
    return data if isinstance(data, dict) else {}


def _builder_step_session_save_context(*, context_file: Optional[str], context: dict[str, Any]) -> None:
    path_text = str(context_file or "").strip()
    if not path_text:
        return
    _write_json_atomic(Path(path_text), dict(context or {}))


def _builder_step_session_record_result(*, session_id: Optional[str], result: dict[str, Any]) -> None:
    sid = _normalize_builder_session_id(str(session_id or "").strip())
    if not sid:
        return
    current = _load_builder_session(sid)
    if current is None:
        return
    success = bool(result.get("success"))
    updates = {
        "session_id": sid,
        "status": "active",
        "last_step_name": str(result.get("step_name") or ""),
        "last_step_index": result.get("step_index"),
        "last_run_id": str(result.get("run_id") or current.get("run_id") or ""),
        "last_result": "success" if success else "failed",
        "last_error": str(result.get("error") or "") if not success else "",
        "last_updated_at": _utc_now_iso(),
    }
    _save_builder_session(updates)
    _db_insert_builder_session_step(session_id=sid, result=result)


def _builder_sessions_list(*, pipeline: Optional[str], project_id: Optional[str], env: Optional[str]) -> dict[str, Any]:
    db_sessions = _db_list_builder_sessions(pipeline=pipeline, project_id=project_id, env=env)
    if db_sessions is not None:
        return {"sessions": db_sessions}
    root = _builder_sessions_root()
    pip = str(pipeline or "").strip()
    pid = normalize_project_id(project_id) if project_id else None
    env_name = str(env or "").strip()
    sessions: list[dict[str, Any]] = []
    for path in sorted(root.glob("*/session.json")):
        data = _read_json_file(path, default=None)
        if not isinstance(data, dict):
            continue
        sid = path.parent.name
        data["session_id"] = sid
        if pid and normalize_project_id(data.get("project_id")) != pid:
            continue
        if pip and str(data.get("pipeline") or "").strip() != pip:
            continue
        if env_name and str(data.get("env") or "").strip() != env_name:
            continue
        if not str(data.get("context_file") or "").strip():
            data["context_file"] = _builder_session_context_file(sid).as_posix()
        sessions.append(data)
    sessions.sort(key=lambda s: str(s.get("updated_at") or s.get("created_at") or ""), reverse=True)
    return {"sessions": sessions}


def _builder_session_get(session_id: str) -> dict[str, Any]:
    sid = _normalize_builder_session_id(session_id)
    session = _load_builder_session(sid)
    if not session:
        raise HTTPException(status_code=404, detail=f"Builder session not found: {sid}")
    return {"session": session}


def _release_local_run(run_id: str) -> None:
    with _LOCAL_RUN_LOCK:
        key = _LOCAL_RUN_KEY_BY_RUN_ID.pop(run_id, None)
        if key:
            _ACTIVE_LOCAL_RUN_KEYS.pop(key, None)
        _LOCAL_RUN_FUTURES.pop(run_id, None)
        _LOCAL_RUN_CANCEL_REQUESTED.discard(run_id)


def _submit_local_run_async(
    *,
    run_id: str,
    dedupe_key: str,
    executor: LocalExecutor,
    pipeline_path: Path,
    context: dict[str, Any],
    project_id: Optional[str],
) -> None:
    live_log_file = (executor.workdir / "_live" / f"{run_id}.log").resolve().as_posix()
    _set_local_run_snapshot(
        run_id,
        state="queued",
        pipeline=str(pipeline_path),
        executor="local",
        project_id=project_id,
        log_file=live_log_file,
    )
    _append_local_run_log(run_id, "Run queued from web UI.", "INFO")
    try:
        upsert_run_status(
            run_id=run_id,
            pipeline=str(pipeline_path),
            status="queued",
            success=False,
            message="queued from web UI",
            executor="local",
            project_id=project_id,
            provenance=context.get("provenance"),
            event_type="run_queued",
            event_details={"source": "web"},
        )
    except Exception:
        pass

    def _worker() -> None:
        with _LOCAL_RUN_LOCK:
            if run_id in _LOCAL_RUN_CANCEL_REQUESTED:
                _set_local_run_snapshot(run_id, state="cancelled", message="Cancelled before execution started.")
                _append_local_run_log(run_id, "Run cancelled before execution started.", "WARN")
                try:
                    upsert_run_status(
                        run_id=run_id,
                        pipeline=str(pipeline_path),
                        status="cancelled",
                        success=False,
                        message="cancelled before execution started",
                        executor="local",
                        project_id=project_id,
                        provenance=context.get("provenance"),
                        event_type="run_cancelled",
                        event_details={"source": "web"},
                    )
                except Exception:
                    pass
                _release_local_run(run_id)
                return
        _set_local_run_snapshot(run_id, state="running")
        _append_local_run_log(run_id, "Run started.", "INFO")
        run_context = dict(context or {})
        run_context["log"] = lambda msg, level="INFO": _append_local_run_log(run_id, str(msg), str(level or "INFO"))
        run_context["step_log"] = (
            lambda step_name, msg, level="INFO": _append_local_run_log(
                run_id,
                f"[{step_name}] {str(msg)}",
                str(level or "INFO"),
            )
        )
        try:
            upsert_run_status(
                run_id=run_id,
                pipeline=str(pipeline_path),
                status="running",
                success=False,
                message="running from web UI",
                executor="local",
                project_id=project_id,
                provenance=context.get("provenance"),
                event_type="run_started",
                event_details={"source": "web"},
            )
        except Exception:
            pass

        try:
            submit = executor.submit(str(pipeline_path), context=run_context)
            status = executor.status(submit.run_id)
            state = status.state.value if hasattr(status.state, "value") else str(status.state)
            _set_local_run_snapshot(run_id, state=state, message=status.message or submit.message or "")
            _append_local_run_log(run_id, f"Run finished with state={state}.", "INFO")
        except Exception as exc:  # noqa: BLE001
            _set_local_run_snapshot(run_id, state="failed", message=str(exc))
            _append_local_run_log(run_id, f"Run failed: {exc}", "ERROR")
            try:
                upsert_run_status(
                    run_id=run_id,
                    pipeline=str(pipeline_path),
                    status="failed",
                    success=False,
                    message=str(exc),
                    executor="local",
                    project_id=project_id,
                    provenance=context.get("provenance"),
                    event_type="run_failed",
                    event_details={"source": "web", "error": str(exc)},
                )
            except Exception:
                pass
        finally:
            _release_local_run(run_id)

    future = _LOCAL_RUN_POOL.submit(_worker)
    with _LOCAL_RUN_LOCK:
        _LOCAL_RUN_KEY_BY_RUN_ID[run_id] = dedupe_key
        _LOCAL_RUN_FUTURES[run_id] = future


@app.get("/api/health")
def health() -> dict:
    return {"ok": True}


def _normalize_user_id(value: Optional[str]) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    # Keep IDs stable and resilient to hidden punctuation/whitespace noise.
    raw = re.sub(r"[^a-z0-9-]+", "", raw)
    return raw or "admin"


def _is_admin_user(user_id: str) -> bool:
    normalized = _normalize_user_id(user_id)
    return normalized in {"admin", "administrator", "root"}


def _static_user_projects(user_id: str) -> set[str]:
    mapping = {
        "admin": {"land_core", "gee_lee"},
        "land-core": {"land_core"},
        "gee-lee": {"gee_lee"},
    }
    return set(mapping.get(user_id, set()))


def _load_user_projects_from_db(user_id: str) -> Optional[set[str]]:
    db_url = get_database_url()
    if not db_url:
        return None
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT up.project_id
                    FROM etl_user_projects up
                    JOIN etl_projects p ON p.project_id = up.project_id
                    WHERE up.user_id = %s
                      AND p.is_active = TRUE
                    """,
                    (user_id,),
                )
                rows = cur.fetchall() or []
        return {str(r[0]) for r in rows if str(r[0]).strip()}
    except Exception:
        return None


def _resolve_user_scope(request: Request) -> UserScope:
    raw_user = request.headers.get("X-ETL-User") or request.query_params.get("as_user")
    user_id = _normalize_user_id(raw_user)
    projects = _load_user_projects_from_db(user_id)
    if projects is None:
        projects = _static_user_projects(user_id)
    normalized_projects: set[str] = set()
    for raw_pid in set(projects or set()):
        pid = normalize_project_id(raw_pid)
        if not pid or pid == "default":
            continue
        normalized_projects.add(pid)
    projects = normalized_projects
    if not projects:
        raise HTTPException(status_code=403, detail=f"User has no project access: {user_id}")
    return UserScope(user_id=user_id, allowed_projects=projects)


def _require_project_access(scope: UserScope, project_id: Optional[str]) -> Optional[str]:
    normalized = normalize_project_id(project_id)
    if not normalized:
        return None
    if _is_admin_user(scope.user_id):
        return normalized
    if normalized not in scope.allowed_projects:
        raise HTTPException(
            status_code=403,
            detail=f"User '{scope.user_id}' is not allowed for project '{normalized}'.",
        )
    return normalized


def _combine_project_scoped_rows(
    rows_by_project: list[list[dict[str, Any]]],
    *,
    limit: int,
    dedupe_key: str,
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rows in rows_by_project:
        for row in rows:
            key = str(row.get(dedupe_key) or "")
            if key and key in seen:
                continue
            if key:
                seen.add(key)
            merged.append(row)
    merged.sort(key=lambda r: str(r.get("started_at") or r.get("last_started_at") or ""), reverse=True)
    return merged[:limit]


def _resolve_request_project_filter(
    *,
    request: Request,
    project_id: Optional[str],
    pipeline_id: Optional[str] = None,
) -> tuple[UserScope, Optional[str]]:
    scope = _resolve_user_scope(request)
    requested = normalize_project_id(project_id)
    inferred = normalize_project_id(infer_project_id_from_pipeline_path(pipeline_id)) if pipeline_id else None
    if requested and inferred and requested != inferred:
        raise HTTPException(
            status_code=400,
            detail=f"project_id '{requested}' does not match pipeline path project '{inferred}'.",
        )
    selected = _require_project_access(scope, requested or inferred)
    if selected:
        return scope, selected
    if len(scope.allowed_projects) == 1:
        return scope, sorted(scope.allowed_projects)[0]
    return scope, None


def _parse_dataset_create_payload(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    payload = payload or {}
    dataset_id = str(payload.get("dataset_id") or "").strip()
    if not dataset_id:
        raise HTTPException(status_code=400, detail="`dataset_id` is required.")
    return {
        "dataset_id": dataset_id,
        "data_class": str(payload.get("data_class") or "").strip() or None,
        "owner_user": str(payload.get("owner_user") or "").strip() or None,
        "status": str(payload.get("status") or "").strip() or None,
    }


def _fetch_executor_capabilities() -> list[dict[str, Any]]:
    repo_root = Path(".").resolve()
    executors = [
        LocalExecutor(),
        SlurmExecutor(env_config={}, repo_root=repo_root, dry_run=True),
        HpccDirectExecutor(env_config={}, repo_root=repo_root, dry_run=True),
    ]
    return [
        {
            "executor": ex.name,
            "capabilities": ex.capabilities(),
        }
        for ex in executors
    ]


app.include_router(
    build_api_read_router(
        resolve_user_scope=_resolve_user_scope,
        resolve_request_project_filter=_resolve_request_project_filter,
        require_project_access=_require_project_access,
        combine_project_scoped_rows=_combine_project_scoped_rows,
        parse_dataset_create_payload=_parse_dataset_create_payload,
        fetch_pipelines=lambda **kwargs: fetch_pipelines(**kwargs),
        fetch_pipeline_runs=lambda *args, **kwargs: _fetch_pipeline_runs_with_fallback(*args, **kwargs),
        fetch_pipeline_validations=lambda *args, **kwargs: fetch_pipeline_validations(*args, **kwargs),
        fetch_pipeline_detail=lambda *args, **kwargs: fetch_pipeline_detail(*args, **kwargs),
        fetch_datasets=lambda **kwargs: fetch_datasets(**kwargs),
        fetch_dataset_detail=lambda *args, **kwargs: fetch_dataset_detail(*args, **kwargs),
        fetch_runs=lambda **kwargs: fetch_runs(**kwargs),
        fetch_run_detail=lambda *args, **kwargs: fetch_run_detail(*args, **kwargs),
        fetch_executor_capabilities=lambda: _fetch_executor_capabilities(),
        create_dataset=lambda **kwargs: create_dataset(**kwargs),
        WebQueryError=WebQueryError,
        DatasetServiceError=DatasetServiceError,
    )
)
app.include_router(
    build_api_actions_router(
        payload_with_pipeline=lambda payload, pipeline_id: _payload_with_pipeline(payload, pipeline_id),
        action_validate=lambda request, payload: api_action_validate(request, payload),
        action_run=lambda request, payload: api_action_run(request, payload),
        stop_run=lambda run_id, request, payload: api_stop_run(run_id, request, payload),
        resume_run=lambda run_id, request, payload: api_resume_run(run_id, request, payload),
    )
)
app.include_router(
    build_api_run_artifacts_router(
        run_files=lambda run_id, request: api_run_files(run_id, request),
        run_file=lambda run_id, request, path: api_run_file(run_id, request, path),
        run_live_log=lambda run_id, request, limit: api_run_live_log(run_id, request, limit),
    )
)
app.include_router(
    build_api_query_router(
        query_preview=lambda request, payload: api_query_preview(request, payload),
    )
)
app.include_router(
    build_api_builder_router(
        builder_source=lambda **kwargs: api_builder_source(**kwargs),
        builder_files=lambda **kwargs: api_builder_files(**kwargs),
        builder_plugins=lambda **kwargs: api_builder_plugins(**kwargs),
        builder_environments=lambda **kwargs: api_builder_environments(**kwargs),
        builder_projects=lambda **kwargs: api_builder_projects(**kwargs),
        builder_project_vars=lambda **kwargs: api_builder_project_vars(**kwargs),
        builder_git_status=lambda **kwargs: api_builder_git_status(**kwargs),
        builder_git_main_check=lambda **kwargs: api_builder_git_main_check(**kwargs),
        builder_git_sync=lambda **kwargs: api_builder_git_sync(**kwargs),
        builder_resolve_text=lambda **kwargs: api_builder_resolve_text(**kwargs),
        builder_namespace=lambda **kwargs: api_builder_namespace(**kwargs),
        builder_validate=lambda **kwargs: api_builder_validate(**kwargs),
        builder_generate=lambda **kwargs: api_builder_generate(**kwargs),
        builder_test_step=lambda **kwargs: api_builder_test_step(**kwargs),
        builder_test_step_start=lambda **kwargs: api_builder_test_step_start(**kwargs),
        builder_test_step_status=lambda **kwargs: api_builder_test_step_status(**kwargs),
        builder_test_step_stop=lambda **kwargs: api_builder_test_step_stop(**kwargs),
        builder_sessions_list=lambda **kwargs: api_builder_sessions_list(**kwargs),
        builder_sessions_create=lambda **kwargs: api_builder_sessions_create(**kwargs),
        builder_sessions_get=lambda **kwargs: api_builder_sessions_get(**kwargs),
    )
)
app.include_router(
    build_api_management_router(
        pipelines_create=lambda request, payload: api_pipelines_create(request, payload),
        pipelines_update=lambda request, pipeline_id, payload: api_pipelines_update(request, pipeline_id, payload),
        project_dag=lambda **kwargs: api_project_dag(**kwargs),
        plugins_stats=lambda **kwargs: api_plugins_stats(**kwargs),
    )
)


def _parse_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: must be an integer.") from exc


def _parse_optional_float(value: Any, *, field_name: str) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}: must be a number.") from exc


def _git_out(repo_root: Path, *args: str) -> str:
    proc = run_logged_subprocess(
        ["git", "-C", str(repo_root), *args],
        logger=_LOG,
        action="web_api.git",
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise HTTPException(status_code=400, detail=f"Git command failed: {' '.join(args)}: {detail}")
    return str(proc.stdout or "").strip()


def _git_repo_status(repo_root: Optional[Path] = None) -> dict[str, Any]:
    root = (repo_root or Path(".")).resolve()
    is_repo = run_logged_subprocess(
        ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
        logger=_LOG,
        action="web_api.git",
        check=False,
    )
    if is_repo.returncode != 0:
        raise HTTPException(status_code=400, detail="Current workspace is not a git repository.")
    origin = _git_out(root, "config", "--get", "remote.origin.url")
    branch = _git_out(root, "rev-parse", "--abbrev-ref", "HEAD")
    commit = _git_out(root, "rev-parse", "HEAD")
    dirty_proc = run_logged_subprocess(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
        logger=_LOG,
        action="web_api.git",
        check=False,
    )
    dirty = bool(str(dirty_proc.stdout or "").strip()) if dirty_proc.returncode == 0 else False
    return {
        "repo_root": str(root),
        "origin_url": origin or None,
        "repo_name": infer_repo_name(origin or root.name),
        "branch": branch or None,
        "commit": commit or None,
        "dirty": dirty,
    }


def _git_branch_slug(text: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._/-]+", "-", str(text or "").strip())
    slug = re.sub(r"/{2,}", "/", slug).strip("./-")
    return slug or "builder"


def _builder_git_sync_repo_root_from_env() -> Optional[Path]:
    raw = str(os.environ.get("ETL_BUILDER_GIT_SYNC_REPO") or "").strip()
    if not raw:
        return None
    root = Path(raw).expanduser().resolve()
    if not root.exists():
        raise HTTPException(
            status_code=400,
            detail=f"Configured ETL_BUILDER_GIT_SYNC_REPO does not exist: {root}",
        )
    is_repo = run_logged_subprocess(
        ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
        logger=_LOG,
        action="web_api.git",
        check=False,
    )
    if is_repo.returncode != 0:
        raise HTTPException(
            status_code=400,
            detail=f"Configured ETL_BUILDER_GIT_SYNC_REPO is not a git repository: {root}",
        )
    return root


def _builder_git_sync_repo_root_from_project_source(
    *,
    project_id: Optional[str],
    projects_config: Optional[str],
    pipeline_source: Optional[str],
) -> Optional[Path]:
    pid = normalize_project_id(project_id)
    if not pid:
        return None
    repo_root = Path(".").resolve()
    _pid, project_vars, _cfg = _builder_project_context(project_id=pid, projects_config=projects_config)
    views, _warnings = _builder_pipeline_source_views(project_vars=project_vars, repo_root=repo_root)
    if not views:
        return None
    requested = str(pipeline_source or "").strip()
    if requested:
        selected = next((v for v in views if str(v.get("label") or "") == requested), None)
        if selected is None:
            raise HTTPException(status_code=400, detail=f"Unknown pipeline_source '{requested}'.")
        return Path(selected["repo_root"]).resolve()
    if len(views) == 1:
        return Path(views[0]["repo_root"]).resolve()
    labels = [str(v.get("label") or "") for v in views]
    raise HTTPException(
        status_code=400,
        detail=f"Multiple project pipeline sources available {labels}. Pass pipeline_source for git sync.",
    )


def _builder_git_sync(
    *,
    pipeline: str,
    branch: Optional[str] = None,
    push: bool = True,
    create_branch: bool = True,
    publish_to_main: bool = False,
    checkout_main_after_publish: bool = True,
    project_id: Optional[str] = None,
    projects_config: Optional[str] = None,
    pipeline_source: Optional[str] = None,
) -> dict[str, Any]:
    repo_root = _builder_git_sync_repo_root_from_project_source(
        project_id=project_id,
        projects_config=projects_config,
        pipeline_source=pipeline_source,
    )
    if repo_root is None:
        repo_root = _builder_git_sync_repo_root_from_env()
    if repo_root is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "Builder git sync is disabled for this repo by default. "
                "Set ETL_BUILDER_GIT_SYNC_REPO to your pipelines/scripts repository path."
            ),
        )
    status_before = _git_repo_status(repo_root)
    pipeline_rel = str(pipeline or "").strip().replace("\\", "/")
    if not pipeline_rel:
        raise HTTPException(status_code=400, detail="`pipeline` is required for git sync.")
    if not pipeline_rel.lower().startswith("pipelines/"):
        pipeline_rel = f"pipelines/{pipeline_rel.lstrip('/')}"
    if not pipeline_rel.lower().endswith((".yml", ".yaml")):
        pipeline_rel = f"{pipeline_rel}.yml"

    if branch and str(branch).strip():
        target_branch = _git_branch_slug(str(branch).strip())
    else:
        day = datetime.utcnow().strftime("%y%m%d")
        # Reuse one branch per day across builder edits so switching pipelines/plugins
        # does not jump to fresh per-pipeline branches.
        target_branch = f"builder/day-{day}" if create_branch else str(status_before.get("branch") or "builder")
    current_branch = str(status_before.get("branch") or "").strip()

    if target_branch and target_branch != current_branch:
        exists_local = run_logged_subprocess(
            ["git", "-C", str(repo_root), "show-ref", "--verify", "--quiet", f"refs/heads/{target_branch}"],
            logger=_LOG,
            action="web_api.git",
            check=False,
        )
        if exists_local.returncode == 0:
            _git_out(repo_root, "checkout", target_branch)
        else:
            exists_remote = run_logged_subprocess(
                ["git", "-C", str(repo_root), "show-ref", "--verify", "--quiet", f"refs/remotes/origin/{target_branch}"],
                logger=_LOG,
                action="web_api.git",
                check=False,
            )
            if exists_remote.returncode == 0:
                _git_out(repo_root, "checkout", "-B", target_branch, f"origin/{target_branch}")
            else:
                _git_out(repo_root, "checkout", "-b", target_branch)

    _git_out(repo_root, "add", "--", "pipelines", "scripts")
    staged = _git_out(repo_root, "diff", "--cached", "--name-only")
    staged_files = [x.strip() for x in staged.splitlines() if x.strip()]
    committed = False
    if staged_files:
        msg = f"builder sync: {pipeline_rel}"
        _git_out(repo_root, "commit", "-m", msg)
        committed = True

    if push:
        _git_out(repo_root, "push", "-u", "origin", target_branch)

    published = False
    main_branch = "main"
    if publish_to_main:
        if not target_branch:
            raise HTTPException(status_code=400, detail="publish_to_main requires a target builder branch.")
        _git_out(repo_root, "checkout", main_branch)
        _git_out(repo_root, "pull", "--ff-only", "origin", main_branch)
        _git_out(repo_root, "merge", "--no-ff", "--no-edit", target_branch)
        if push:
            _git_out(repo_root, "push", "origin", main_branch)
        published = True
        if not checkout_main_after_publish:
            _git_out(repo_root, "checkout", target_branch)

    status_after = _git_repo_status(repo_root)
    return {
        "pipeline": pipeline_rel,
        "branch": target_branch,
        "committed": committed,
        "staged_files": staged_files,
        "pushed": bool(push),
        "published_to_main": bool(published),
        "main_branch": main_branch,
        "checkout_main_after_publish": bool(checkout_main_after_publish),
        "repo_root": str(repo_root),
        "status": status_after,
    }


def _builder_git_target_repo_root(
    *,
    project_id: Optional[str],
    projects_config: Optional[str],
    pipeline_source: Optional[str],
) -> tuple[Path, bool]:
    repo_root = _builder_git_sync_repo_root_from_project_source(
        project_id=project_id,
        projects_config=projects_config,
        pipeline_source=pipeline_source,
    )
    from_project_source = repo_root is not None
    if repo_root is None:
        repo_root = _builder_git_sync_repo_root_from_env()
    if repo_root is None:
        repo_root = Path(".").resolve()
    return repo_root, from_project_source


def _git_main_health(root: Path) -> dict[str, Any]:
    status = _git_repo_status(root)
    branch = str(status.get("branch") or "").strip()
    dirty = bool(status.get("dirty"))
    is_main_branch = branch == "main"

    fetch_proc = run_logged_subprocess(
        ["git", "-C", str(root), "fetch", "--quiet", "origin", "main"],
        logger=_LOG,
        action="web_api.git",
        check=False,
    )
    remote_ref_ok = fetch_proc.returncode == 0
    ahead = None
    behind = None
    in_sync_with_origin_main = None
    if remote_ref_ok:
        counts = _git_out(root, "rev-list", "--left-right", "--count", "origin/main...HEAD")
        parts = [x for x in str(counts or "").strip().split() if x.strip()]
        if len(parts) >= 2:
            behind = int(parts[0])
            ahead = int(parts[1])
            in_sync_with_origin_main = behind == 0 and ahead == 0

    ok = bool(is_main_branch and (not dirty) and (in_sync_with_origin_main is True))
    detail = []
    if not is_main_branch:
        detail.append(f"Current branch is '{branch or '<unknown>'}', expected 'main'.")
    if dirty:
        detail.append("Working tree has uncommitted tracked changes.")
    if in_sync_with_origin_main is False:
        detail.append(f"Local main differs from origin/main (ahead={ahead}, behind={behind}).")
    if in_sync_with_origin_main is None:
        detail.append("Could not verify origin/main sync (git fetch origin main failed).")

    status["is_main_branch"] = is_main_branch
    status["ahead_of_origin_main"] = ahead
    status["behind_origin_main"] = behind
    status["in_sync_with_origin_main"] = in_sync_with_origin_main
    return {
        "ok": ok,
        "detail": " ".join(detail).strip(),
        "status": status,
    }


def _pipeline_name_variants(pipeline_id: str) -> list[str]:
    p = str(pipeline_id or "").strip().replace("\\", "/")
    if not p:
        return []
    variants: list[str] = []
    variants.append(p)
    if p.lower().startswith("pipelines/"):
        variants.append(p[len("pipelines/"):])
    else:
        variants.append(f"pipelines/{p}")
    # Preserve order while de-duplicating.
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        key = v.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _fetch_pipeline_runs_local_fallback(
    pipeline_id: str,
    *,
    limit: int = 50,
    status: Optional[str] = None,
    executor: Optional[str] = None,
    project_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    if executor:
        # Local JSONL run records currently do not include executor.
        return []
    variants = set(_pipeline_name_variants(pipeline_id))
    if not variants:
        return []
    store = (Path(".") / ".runs" / "runs.jsonl").resolve()
    recs = load_runs(store)
    rows: list[dict[str, Any]] = []
    for rec in recs:
        pipeline = str(getattr(rec, "pipeline", "") or "").strip().replace("\\", "/")
        if pipeline not in variants:
            continue
        if project_id and str(getattr(rec, "project_id", "") or "").strip() != str(project_id).strip():
            continue
        rec_status = str(getattr(rec, "status", "") or "").strip()
        if status and rec_status != str(status).strip():
            continue
        rows.append(
            {
                "run_id": str(getattr(rec, "run_id", "") or ""),
                "pipeline": pipeline,
                "project_id": str(getattr(rec, "project_id", "") or "") or None,
                "status": rec_status,
                "success": bool(getattr(rec, "success", False)),
                "started_at": str(getattr(rec, "started_at", "") or "") or None,
                "ended_at": str(getattr(rec, "ended_at", "") or "") or None,
                "message": str(getattr(rec, "message", "") or ""),
                "executor": "",
                "artifact_dir": None,
            }
        )
    rows.sort(key=lambda r: DateTimeSafe.parse(str(r.get("started_at") or "")), reverse=True)
    return rows[: max(1, min(int(limit), 500))]


class DateTimeSafe:
    @staticmethod
    def parse(raw: str) -> datetime:
        text = str(raw or "").strip()
        if not text:
            return datetime.fromtimestamp(0, tz=timezone.utc)
        try:
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return datetime.fromtimestamp(0, tz=timezone.utc)


def _fetch_pipeline_runs_with_fallback(
    pipeline_id: str,
    *,
    limit: int = 50,
    status: Optional[str] = None,
    executor: Optional[str] = None,
    project_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        # Query DB with both normalized path forms.
        seen: set[str] = set()
        for p in _pipeline_name_variants(pipeline_id):
            db_rows = fetch_pipeline_runs(
                p,
                limit=limit,
                status=status,
                executor=executor,
                project_id=project_id,
            )
            for row in db_rows:
                rid = str((row or {}).get("run_id") or "").strip()
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                rows.append(row)
    except WebQueryError:
        rows = []

    local_rows = _fetch_pipeline_runs_local_fallback(
        pipeline_id,
        limit=limit,
        status=status,
        executor=executor,
        project_id=project_id,
    )
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows + local_rows:
        rid = str((row or {}).get("run_id") or "").strip()
        if not rid:
            continue
        by_id[rid] = row
    merged = list(by_id.values())
    merged.sort(key=lambda r: DateTimeSafe.parse(str((r or {}).get("started_at") or "")), reverse=True)
    return merged[: max(1, min(int(limit), 500))]


def _action_handler_deps() -> dict[str, Any]:
    return {
        "parse_bool": _parse_bool,
        "parse_optional_int": _parse_optional_int,
        "parse_optional_float": _parse_optional_float,
        "normalize_project_id": normalize_project_id,
        "resolve_execution_config_path": resolve_execution_config_path,
        "load_execution_config": load_execution_config,
        "ExecutionConfigError": ExecutionConfigError,
        "validate_environment_executor": validate_environment_executor,
        "apply_execution_env_overrides": apply_execution_env_overrides,
        "resolve_execution_env_templates": resolve_execution_env_templates,
        "resolve_projects_config_path": resolve_projects_config_path,
        "ProjectConfigError": ProjectConfigError,
        "resolve_project_id": resolve_project_id,
        "load_project_vars": load_project_vars,
        "resolve_project_writable_pipeline_path": _resolve_project_writable_pipeline_path,
        "resolve_pipeline_path_from_project_sources": resolve_pipeline_path_from_project_sources,
        "PipelineAssetError": PipelineAssetError,
        "parse_pipeline": parse_pipeline,
        "PipelineError": PipelineError,
        "resolve_global_vars": _resolve_global_vars,
        "resolve_user_scope": _resolve_user_scope,
        "require_project_access": _require_project_access,
        "build_web_request_solver": _build_web_request_solver,
        "record_pipeline_validation": _record_pipeline_validation,
        "infer_external_pipeline_remote_hint": _infer_external_pipeline_remote_hint,
        "resolve_workdir_from_solver": _resolve_workdir_from_solver,
        "resolve_text_with_ctx_iterative": _resolve_text_with_ctx_iterative,
        "DEFAULT_RESOLVE_MAX_PASSES": DEFAULT_RESOLVE_MAX_PASSES,
        "collect_run_provenance": collect_run_provenance,
        "SlurmExecutor": SlurmExecutor,
        "HpccDirectExecutor": HpccDirectExecutor,
        "LocalExecutor": LocalExecutor,
        "local_submission_key": _local_submission_key,
        "submit_local_run_async": _submit_local_run_async,
        "LOCAL_RUN_LOCK": _LOCAL_RUN_LOCK,
        "ACTIVE_LOCAL_RUN_KEYS": _ACTIVE_LOCAL_RUN_KEYS,
        "LOCAL_RUN_SNAPSHOT": _LOCAL_RUN_SNAPSHOT,
        "LOCAL_RUN_FUTURES": _LOCAL_RUN_FUTURES,
        "LOCAL_RUN_CANCEL_REQUESTED": _LOCAL_RUN_CANCEL_REQUESTED,
        "set_local_run_snapshot": _set_local_run_snapshot,
        "append_local_run_log": _append_local_run_log,
        "release_local_run": _release_local_run,
        "upsert_run_status": upsert_run_status,
        "fetch_run_header": fetch_run_header,
        "WebQueryError": WebQueryError,
    }


def _run_artifact_handler_deps() -> dict[str, Any]:
    return {
        "fetch_run_header": fetch_run_header,
        "WebQueryError": WebQueryError,
        "resolve_user_scope": _resolve_user_scope,
        "require_project_access": _require_project_access,
        "SlurmExecutor": SlurmExecutor,
        "LocalExecutor": LocalExecutor,
        "MAX_FILE_VIEW_BYTES": MAX_FILE_VIEW_BYTES,
        "LOCAL_RUN_LOCK": _LOCAL_RUN_LOCK,
        "LOCAL_RUN_SNAPSHOT": _LOCAL_RUN_SNAPSHOT,
        "LOCAL_RUN_LOG_RING": _LOCAL_RUN_LOG_RING,
        "tail_text_lines": _tail_text_lines,
    }


def _query_handler_deps() -> dict[str, Any]:
    return {
        "normalize_project_id": normalize_project_id,
        "resolve_user_scope": _resolve_user_scope,
        "require_project_access": _require_project_access,
        "resolve_global_vars": _resolve_global_vars,
        "resolve_execution_env": _resolve_execution_env,
        "resolve_execution_config_path": resolve_execution_config_path,
        "load_execution_config": load_execution_config,
        "ExecutionConfigError": ExecutionConfigError,
        "LocalExecutor": LocalExecutor,
        "SlurmExecutor": SlurmExecutor,
        "HpccDirectExecutor": HpccDirectExecutor,
        "QueryError": QueryError,
    }


def _parse_action_payload(payload: Optional[dict[str, Any]]) -> dict[str, Any]:
    return web_action_handlers.parse_action_payload(payload, _action_handler_deps())


def _resolve_global_vars(global_config_path: Optional[Path]) -> dict[str, Any]:
    try:
        resolved = resolve_global_config_path(global_config_path)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Global config error: {exc}") from exc
    if not resolved:
        return {}
    try:
        return load_global_config(resolved)
    except ConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Global config error: {exc}") from exc


def _resolve_execution_env(
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    *,
    executor: str,
    global_vars: Optional[dict[str, Any]] = None,
) -> tuple[dict[str, Any], Optional[Path], Optional[str]]:
    return web_action_handlers.resolve_execution_env(
        environments_config_path,
        env_name,
        executor=executor,
        global_vars=global_vars,
        deps=_action_handler_deps(),
    )


def _resolve_action_pipeline_context(
    *,
    pipeline_path: Path,
    requested_project_id: Optional[str],
    pipeline_source: Optional[str],
    projects_config_path: Optional[Path],
    global_vars: dict[str, Any],
    execution_env: dict[str, Any],
) -> tuple[Path, str, dict[str, Any], Optional[Path]]:
    return web_action_handlers.resolve_action_pipeline_context(
        pipeline_path=pipeline_path,
        requested_project_id=requested_project_id,
        pipeline_source=pipeline_source,
        projects_config_path=projects_config_path,
        global_vars=global_vars,
        execution_env=execution_env,
        deps=_action_handler_deps(),
    )


def _resolve_builder_env_vars(
    *,
    environments_config_path: Optional[Path],
    env_name: Optional[str],
    global_vars: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    name = str(env_name or "").strip()
    if not name:
        return {}
    try:
        resolved = resolve_execution_config_path(environments_config_path)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    if not resolved:
        if name == "local":
            return {}
        raise HTTPException(status_code=400, detail="Environment selected but environments config was not found.")
    try:
        envs = load_execution_config(resolved)
    except ExecutionConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Environments config error: {exc}") from exc
    env = envs.get(name)
    if not isinstance(env, dict):
        if name == "local":
            return {}
        raise HTTPException(status_code=400, detail=f"Execution env '{name}' not found in config.")
    resolved_env = apply_execution_env_overrides(dict(env))
    return resolve_execution_env_templates(resolved_env, global_vars=global_vars or {})


def _resolve_builder_project_vars(
    yaml_text: str,
    *,
    explicit_project_id: Optional[str] = None,
    projects_config_path: Optional[Path] = None,
    pipeline_hint: Optional[str] = None,
) -> tuple[Optional[str], dict[str, Any]]:
    try:
        import yaml

        raw = yaml.safe_load(yaml_text) or {}
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    yaml_project_id = normalize_project_id(str(raw.get("project_id") or "").strip() or None)
    if not yaml_project_id:
        meta = raw.get("metadata")
        if isinstance(meta, dict):
            yaml_project_id = normalize_project_id(str(meta.get("project_id") or "").strip() or None)
    inferred_path: Optional[Path] = None
    hint = str(pipeline_hint or "").strip()
    if hint:
        try:
            inferred_path = _resolve_repo_relative_pipeline_path(hint)
        except Exception:
            inferred_path = None
    project_id = resolve_project_id(
        explicit_project_id=normalize_project_id(str(explicit_project_id or "").strip() or None),
        pipeline_project_id=yaml_project_id,
        pipeline_path=inferred_path,
    )
    try:
        resolved_projects_config = resolve_projects_config_path(projects_config_path)
        project_vars = load_project_vars(project_id=project_id, projects_config_path=resolved_projects_config)
    except ProjectConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    return project_id, project_vars


def _parse_pipeline_from_yaml_text(
    yaml_text: str,
    *,
    global_config_path: Optional[Path],
    environments_config_path: Optional[Path] = None,
    env_name: Optional[str] = None,
    project_id: Optional[str] = None,
    projects_config_path: Optional[Path] = None,
    pipeline_hint: Optional[str] = None,
) -> Pipeline:
    if not (yaml_text or "").strip():
        raise HTTPException(status_code=400, detail="`yaml_text` is required.")
    _, project_vars = _resolve_builder_project_vars(
        yaml_text,
        explicit_project_id=project_id,
        projects_config_path=projects_config_path,
        pipeline_hint=pipeline_hint,
    )
    global_vars = _resolve_global_vars(global_config_path)
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
        global_vars=global_vars,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".yml", delete=False, encoding="utf-8") as tmp:
        tmp.write(yaml_text)
        tmp_path = Path(tmp.name)
    try:
        return parse_pipeline(tmp_path, global_vars=global_vars, env_vars=env_vars, project_vars=project_vars)
    except (PipelineError, FileNotFoundError) as exc:
        raise HTTPException(status_code=400, detail=f"Invalid draft pipeline: {exc}") from exc
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _raw_vars_dirs_from_yaml_text(yaml_text: str) -> tuple[dict[str, Any], dict[str, Any]]:
    import yaml

    try:
        data = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    vars_section = data.get("vars", {}) or {}
    dirs_section = data.get("dirs", {}) or {}
    out_vars = dict(vars_section) if isinstance(vars_section, dict) else {}
    out_dirs = dict(dirs_section) if isinstance(dirs_section, dict) else {}
    return out_vars, out_dirs


def _resolve_builder_plugins_dir(*, global_config_path: Optional[Path], plugins_dir: Optional[str]) -> Path:
    if plugins_dir and str(plugins_dir).strip():
        return Path(str(plugins_dir).strip()).expanduser()
    global_vars = _resolve_global_vars(global_config_path)
    cfg_plugins = str(global_vars.get("plugins_dir") or "").strip()
    if cfg_plugins:
        return Path(cfg_plugins).expanduser()
    return Path("plugins")


def _estimate_from_stats(mean: Optional[Any], std: Optional[Any], samples: Optional[Any], low_mult: float) -> Optional[float]:
    if mean in (None, "") or samples in (None, ""):
        return None
    try:
        mu = float(mean)
        n = int(samples)
    except (TypeError, ValueError):
        return None
    if n <= 0:
        return None
    if n < 5:
        return mu * float(low_mult)
    try:
        sigma = float(std or 0.0)
    except (TypeError, ValueError):
        sigma = 0.0
    return mu + (3.0 * max(0.0, sigma))


def _parse_mem_text_gb(raw: Any) -> Optional[float]:
    text = str(raw or "").strip().lower()
    if not text:
        return None
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([kmgt]?b?)?\s*$", text)
    if not m:
        return None
    num = float(m.group(1))
    unit = str(m.group(2) or "").strip().lower()
    if unit.startswith("k"):
        return num / (1024.0 * 1024.0)
    if unit.startswith("m"):
        return num / 1024.0
    if unit.startswith("t"):
        return num * 1024.0
    return num


def _parse_slurm_time_minutes(raw: Any) -> Optional[float]:
    text = str(raw or "").strip()
    if not text:
        return None
    days = 0
    rest = text
    if "-" in text:
        dpart, rest = text.split("-", 1)
        try:
            days = int(dpart)
        except ValueError:
            return None
    parts = rest.split(":")
    try:
        if len(parts) == 3:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        elif len(parts) == 2:
            h, m, s = 0, int(parts[0]), int(parts[1])
        elif len(parts) == 1:
            h, m, s = 0, int(parts[0]), 0
        else:
            return None
    except ValueError:
        return None
    return float(days * 24 * 60 + h * 60 + m + (s / 60.0))


def _parse_step_script_for_builder(script: str) -> tuple[str, dict[str, Any]]:
    try:
        tokens = shlex.split(str(script or ""))
    except Exception:
        tokens = str(script or "").split()
    if not tokens:
        return "", {}
    plugin_ref = tokens[0]
    params: dict[str, Any] = {}
    for tok in tokens[1:]:
        if "=" in tok:
            k, v = tok.split("=", 1)
            params[k] = v
    return plugin_ref, params


def _pipeline_to_builder_model_from_yaml(yaml_text: str) -> dict[str, Any]:
    import yaml

    try:
        data = yaml.safe_load(yaml_text) or {}
    except yaml.YAMLError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    vars_section = data.get("vars", {}) or {}
    dirs_section = data.get("dirs", {}) or {}
    project_id = str(data.get("project_id") or "").strip()
    metadata_section = data.get("metadata")
    if not project_id and isinstance(metadata_section, dict):
        project_id = str(metadata_section.get("project_id") or "").strip()
    reqs = data.get("requires_pipelines", []) or []
    steps_section = data.get("steps", []) or []

    model_steps: list[dict[str, Any]] = []
    if isinstance(steps_section, list):
        for raw in steps_section:
            if not isinstance(raw, dict):
                continue
            step_map = raw.get("step") if isinstance(raw.get("step"), dict) else raw
            if not isinstance(step_map, dict):
                continue
            plugin_ref = str(step_map.get("plugin") or "").strip()
            params: dict[str, Any] = {}
            args_raw = step_map.get("args")
            if isinstance(args_raw, dict):
                for k, v in args_raw.items():
                    params[str(k)] = v
            if not plugin_ref:
                plugin_ref, script_params = _parse_step_script_for_builder(str(step_map.get("script") or ""))
                params = script_params
            resources: dict[str, Any] = {}
            resources_raw = step_map.get("resources")
            if isinstance(resources_raw, dict):
                for k, v in resources_raw.items():
                    resources[str(k)] = v
            stype = "sequential"
            foreach_raw = str(step_map.get("foreach") or "")
            sequential_foreach_raw = str(step_map.get("sequential_foreach") or "")
            foreach_glob_raw = str(step_map.get("foreach_glob") or "")
            foreach_kind_raw = str(step_map.get("foreach_kind") or "")
            foreach_mode = "var"
            if foreach_glob_raw.strip():
                foreach_mode = "glob"
            if step_map.get("sequential_foreach"):
                stype = "sequential_foreach"
            elif step_map.get("foreach") or step_map.get("foreach_glob"):
                stype = "foreach"
            elif step_map.get("parallel_with"):
                stype = "parallel"
            enabled_raw = step_map.get("enabled")
            if enabled_raw is None and "Enabled" in step_map:
                enabled_raw = step_map.get("Enabled")
            disabled_raw = step_map.get("disabled")
            if disabled_raw is None and "Disabled" in step_map:
                disabled_raw = step_map.get("Disabled")
            enabled = True
            if isinstance(enabled_raw, bool):
                enabled = enabled_raw
            elif isinstance(disabled_raw, bool):
                enabled = not disabled_raw
            model_steps.append(
                {
                    "name": str(step_map.get("name") or ""),
                    "type": stype,
                    "plugin": plugin_ref,
                    "enabled": enabled,
                    "params": params,
                    "resources": resources,
                    "output_var": str(step_map.get("output_var") or ""),
                    "when": str(step_map.get("when") or ""),
                    "parallel_with": str(step_map.get("parallel_with") or ""),
                    "foreach": foreach_raw,
                    "sequential_foreach": sequential_foreach_raw,
                    "foreach_mode": foreach_mode,
                    "foreach_glob": foreach_glob_raw,
                    "foreach_kind": foreach_kind_raw or "dirs",
                }
            )
    return {
        "project_id": project_id,
        "vars": vars_section if isinstance(vars_section, dict) else {},
        "var_types": (
            {
                str(k): (
                    "list"
                    if isinstance(v, list)
                    else "dict"
                    if isinstance(v, dict)
                    else "bool"
                    if isinstance(v, bool)
                    else "number"
                    if isinstance(v, (int, float))
                    else "path"
                    if str(k).strip().lower() in {"workdir", "logdir", "tmpdir", "datadir", "artifactsdir", "bindir"}
                    else "string"
                )
                for k, v in (vars_section.items() if isinstance(vars_section, dict) else [])
            }
        ),
        "dirs": dirs_section if isinstance(dirs_section, dict) else {},
        "requires_pipelines": [str(x) for x in reqs if str(x).strip()] if isinstance(reqs, list) else [],
        "steps": model_steps,
    }


def _validate_draft_yaml(
    yaml_text: str,
    *,
    project_id: Optional[str] = None,
    projects_config_path: Optional[Path] = None,
    pipeline_hint: Optional[str] = None,
) -> tuple[Optional[Pipeline], Optional[str]]:
    try:
        pipeline = _parse_pipeline_from_yaml_text(
            yaml_text,
            global_config_path=None,
            project_id=project_id,
            projects_config_path=projects_config_path,
            pipeline_hint=pipeline_hint,
        )
        return pipeline, None
    except HTTPException as exc:
        return None, str(exc.detail)


def _lookup_ctx_path(ctx: dict[str, Any], dotted: str) -> tuple[Any, bool]:
    cur: Any = ctx
    for part in str(dotted or "").split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
            continue
        return None, False
    return cur, True


def _resolve_text_with_ctx(value: str, ctx: dict[str, Any]) -> str:
    text = str(value or "")

    def _repl(match: re.Match[str]) -> str:
        key = str(match.group(1) or "")
        found, ok = _lookup_ctx_path(ctx, key)
        if not ok or isinstance(found, (dict, list)):
            return match.group(0)
        return str(found)

    return _TPL_RE.sub(_repl, text)


def _resolve_text_with_ctx_iterative(value: str, ctx: dict[str, Any], *, max_passes: int = DEFAULT_RESOLVE_MAX_PASSES) -> str:
    cur = str(value or "")
    for _ in range(max_passes):
        nxt = _resolve_text_with_ctx(cur, ctx)
        if nxt == cur:
            return cur
        cur = nxt
    return cur


def _build_builder_namespace(
    *,
    pipeline: Pipeline,
    global_vars: dict[str, Any],
    env_vars: dict[str, Any],
    project_vars: Optional[dict[str, Any]] = None,
    raw_vars: Optional[dict[str, Any]] = None,
    raw_dirs: Optional[dict[str, Any]] = None,
    preview_run_id: Optional[str] = None,
    preview_run_started: Optional[datetime] = None,
) -> dict[str, Any]:
    def _step_arg_map(script: str) -> dict[str, str]:
        out: dict[str, str] = {}
        try:
            tokens = shlex.split(str(script or ""))
        except Exception:
            tokens = str(script or "").split()
        for tok in tokens[1:]:
            text = str(tok or "")
            if "=" not in text:
                continue
            k, v = text.split("=", 1)
            key = str(k or "").strip()
            if key:
                out[key] = v
        return out

    def _output_var_placeholders(p: Pipeline) -> dict[str, dict[str, Any]]:
        placeholders: dict[str, dict[str, Any]] = {}
        for idx, step in enumerate(p.steps or []):
            output_var = str(step.output_var or "").strip()
            if not output_var:
                continue
            args_map = _step_arg_map(str(step.script or ""))
            ph: dict[str, Any] = {
                "_runtime": True,
                "_producer_step": str(step.name or f"step_{idx}"),
                "_producer_index": idx,
            }
            out_val = str(args_map.get("out") or "").strip()
            if out_val:
                # Common plugin convention for file-producing steps.
                ph["output_dir"] = out_val
                ph["out"] = out_val
            placeholders[output_var] = ph
        return placeholders

    max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)

    def _resolve_preview_flat(
        flat_map: dict[str, Any],
        base_ctx: dict[str, Any],
        *,
        seed_flat: dict[str, Any],
        max_passes: int,
    ) -> tuple[dict[str, Any], int, bool]:
        reserved = {"sys", "global", "globals", "env", "project", "projects", "vars", "dirs", "resolution"}
        current = dict(flat_map or {})
        for i in range(max_passes):
            ctx = dict(base_ctx)
            for k, v in current.items():
                if str(k) not in reserved:
                    ctx[str(k)] = v
            nxt: dict[str, Any] = {}
            for k, v in current.items():
                if isinstance(v, str):
                    ktxt = str(k)
                    temp_ctx = dict(ctx)
                    if ktxt in temp_ctx:
                        temp_ctx.pop(ktxt, None)
                    fallback = seed_flat.get(ktxt)
                    if fallback is not None and not isinstance(fallback, (dict, list)):
                        temp_ctx[ktxt] = fallback
                    resolved = VariableSolver.resolve_iterative(v, temp_ctx, max_passes=1)
                    if isinstance(resolved, (dict, list)):
                        nxt[ktxt] = v
                    else:
                        nxt[ktxt] = str(resolved)
                else:
                    nxt[str(k)] = v
            if nxt == current:
                return current, i + 1, True
            current = nxt
        return current, max_passes, False

    now = preview_run_started or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    run_id_preview = str(preview_run_id or "")
    if not run_id_preview:
        run_id_preview = "run_abcd0123"
    run_short_preview = run_id_preview[:8] if preview_run_id else "abcd0123"
    sys_ns: dict[str, Any] = {
        "run": {
            # Runtime-populated on real execution paths; placeholder values are used in builder preview.
            "id": run_id_preview,
            "short_id": run_short_preview,
        },
        "job": {
            "id": run_id_preview,
            "name": str((pipeline.vars or {}).get("jobname") or ""),
        },
        "step": {
            # Step-specific values are only known during step execution; placeholders provide preview resolution.
            "id": "step_abcd0123",
            "name": "preview_step",
            "index": "0",
        },
        "now": {
            "iso_utc": now.isoformat().replace("+00:00", "Z"),
            "yymmdd": now.strftime("%y%m%d"),
            "hhmmss": now.strftime("%H%M%S"),
            "yymmdd_hhmmss": now.strftime("%y%m%d-%H%M%S"),
        },
    }
    global_ns = dict(global_vars or {})
    env_ns = dict(env_vars or {})
    project_ns = dict(project_vars or {})
    vars_ns = dict(pipeline.vars or {})
    dirs_ns = dict(pipeline.dirs or {})
    vars_preview = dict(raw_vars or vars_ns)
    dirs_preview = dict(raw_dirs or dirs_ns)
    seed_flat: dict[str, Any] = {}
    for source in (global_ns, env_ns, project_ns, vars_preview):
        for k, v in source.items():
            seed_flat[str(k)] = v
    flat_ns: dict[str, Any] = {}
    for source in (seed_flat, dirs_preview):
        for k, v in source.items():
            flat_ns[str(k)] = v
    flat_ns, preview_passes_used, preview_stable = _resolve_preview_flat(
        flat_ns,
        {
            "sys": sys_ns,
            "global": global_ns,
            "globals": global_ns,
            "env": env_ns,
            "project": project_ns,
            "projects": project_ns,
            "pipe": vars_ns,
            "vars": vars_ns,
            "dirs": dirs_ns,
        },
        seed_flat=seed_flat,
        max_passes=max_passes,
    )
    ns: dict[str, Any] = {
        "sys": sys_ns,
        "global": global_ns,
        "env": env_ns,
        "project": project_ns,
        "vars": vars_ns,
        "dirs": dirs_ns,
        "resolution": {
            # vars/dirs are produced by parse_pipeline, which already uses the
            # runtime iterative resolver with this cap.
            "stable": bool(preview_stable),
            "max_passes": int(max_passes),
            "passes_used": int(preview_passes_used),
            "source": "pipeline.parse+preview",
        },
    }
    ns.update({str(k): v for k, v in flat_ns.items()})
    output_ns = _output_var_placeholders(pipeline)
    ns["outputs"] = output_ns
    for name, value in output_ns.items():
        ns[str(name)] = value
    return ns


def _dir_category(key: str) -> Optional[str]:
    k = str(key or "").strip().lower()
    if not k:
        return None
    if k in {"work", "workdir", "work_dir"}:
        return "work"
    if k in {"log", "logdir", "log_dir"}:
        return "log"
    if k in {"artifact", "artifactdir", "artifact_dir"}:
        return "artifact"
    if k in {"stage", "stagedir", "stage_dir"}:
        return "stage"
    if k in {"tmp", "temp", "tmpdir", "tmp_dir", "tempdir", "temp_dir"}:
        return "tmp"
    return None


def _validate_pipeline_dir_contract(pipeline: Pipeline) -> None:
    counts = {"work": 0, "log": 0, "artifact": 0, "stage": 0, "tmp": 0}
    for section in (pipeline.vars or {}, pipeline.dirs or {}):
        for key in section.keys():
            cat = _dir_category(str(key))
            if cat:
                counts[cat] += 1
    errors: list[str] = []
    if counts["work"] != 1:
        errors.append(f"Exactly one work directory is required (found {counts['work']}).")
    if counts["log"] != 1:
        errors.append(f"Exactly one log directory is required (found {counts['log']}).")
    for optional_name in ("artifact", "stage", "tmp"):
        if counts[optional_name] > 1:
            errors.append(f"At most one {optional_name} directory is allowed (found {counts[optional_name]}).")
    if errors:
        raise HTTPException(status_code=400, detail=" ".join(errors))


def _collect_unresolved_step_inputs(pipeline: Pipeline) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for idx, step in enumerate(pipeline.steps or []):
        try:
            tokens = shlex.split(str(step.script or ""))
        except Exception:
            tokens = str(step.script or "").split()
        if tokens:
            plugin_ref = str(tokens[0] or "")
            plugin_tokens = _extract_unresolved_tokens(plugin_ref)
            if plugin_tokens:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": "plugin",
                        "value": plugin_ref,
                        "tokens": plugin_tokens,
                    }
                )
            positional_idx = 0
            for tok in tokens[1:]:
                text = str(tok or "")
                if "=" in text:
                    key, val = text.split("=", 1)
                    field_name = f"args.{str(key or '').strip()}"
                    field_value = val
                else:
                    field_name = f"arg_list[{positional_idx}]"
                    field_value = text
                    positional_idx += 1
                unresolved = _extract_unresolved_tokens(field_value)
                if unresolved:
                    issues.append(
                        {
                            "step_index": idx,
                            "step_name": step.name,
                            "field": field_name,
                            "value": field_value,
                            "tokens": unresolved,
                        }
                    )
        for env_key, env_val in (step.env or {}).items():
            unresolved = _extract_unresolved_tokens(str(env_val or ""))
            if unresolved:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": f"env.{env_key}",
                        "value": str(env_val or ""),
                        "tokens": unresolved,
                    }
                )
        for field_name, field_val in (
            ("when", step.when),
            ("parallel_with", step.parallel_with),
            ("foreach", step.foreach),
            ("foreach_glob", getattr(step, "foreach_glob", None)),
            ("foreach_kind", getattr(step, "foreach_kind", None)),
        ):
            unresolved = _extract_unresolved_tokens(str(field_val or ""))
            if unresolved:
                issues.append(
                    {
                        "step_index": idx,
                        "step_name": step.name,
                        "field": field_name,
                        "value": str(field_val or ""),
                        "tokens": unresolved,
                    }
                )
    return issues


def _filter_builder_unresolved_issues(
    issues: list[dict[str, Any]],
    pipeline: Pipeline,
    namespace: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    prior_output_vars_by_step: dict[int, set[str]] = {}
    seen: set[str] = set()
    for idx, step in enumerate(pipeline.steps or []):
        prior_output_vars_by_step[idx] = set(seen)
        output_var = str(step.output_var or "").strip()
        if output_var:
            seen.add(output_var)

    def _keep_token(tok: Any, *, step_index: int) -> bool:
        text = str(tok or "").strip()
        if not text:
            return False
        if namespace:
            _found, ok = _lookup_ctx_path(namespace, text)
            if ok:
                return False
        # Runtime sys placeholders are valid in builder drafts; they are
        # populated during execution.
        root = text.split(".", 1)[0]
        if root == "sys":
            return False
        if root in {"item", "item_index", "item_name", "item_stem"}:
            return False
        if root in prior_output_vars_by_step.get(int(step_index), set()):
            return False
        return True

    out: list[dict[str, Any]] = []
    for issue in issues or []:
        step_index = int(issue.get("step_index") or 0)
        tokens = [t for t in (issue.get("tokens") or []) if _keep_token(t, step_index=step_index)]
        if not tokens:
            continue
        next_issue = dict(issue)
        next_issue["tokens"] = tokens
        out.append(next_issue)
    return out


def _record_pipeline_validation(
    *,
    pipeline: str,
    project_id: Optional[str] = None,
    valid: bool,
    step_count: int = 0,
    step_names: Optional[list[str]] = None,
    error: Optional[str] = None,
    source: str = "web_validate",
) -> None:
    db_url = get_database_url()
    if not db_url:
        return
    try:
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_pipeline_validations (
                        pipeline, project_id, valid, step_count, step_names_json, error, source
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        pipeline,
                        project_id,
                        bool(valid),
                        int(step_count),
                        json.dumps(step_names or []),
                        error,
                        source,
                    ),
                )
            conn.commit()
    except Exception:
        # Validation history recording should be best-effort and non-blocking.
        pass


def _resolve_repo_relative_pipeline_path(pipeline: str) -> Path:
    raw = (pipeline or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="`pipeline` is required.")
    repo_root = Path(".").resolve()
    pipelines_root = (repo_root / "pipelines").resolve()
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        parts = list(candidate.parts)
        if parts and parts[0].lower() == "pipelines":
            candidate = Path(*parts[1:]) if len(parts) > 1 else Path("")
        if candidate.suffix.lower() not in {".yml", ".yaml"}:
            candidate = candidate.with_suffix(".yml")
        candidate = pipelines_root / candidate
    resolved = candidate.resolve()
    try:
        resolved.relative_to(pipelines_root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Pipeline path must be inside pipelines/: {raw}") from exc
    if resolved.suffix.lower() not in {".yml", ".yaml"}:
        resolved = resolved.with_suffix(".yml")
    return resolved


def _is_local_repo_pipeline_path(path: Path) -> bool:
    repo_root = Path(".").resolve()
    local_pipelines_root = (repo_root / "pipelines").resolve()
    try:
        Path(path).resolve().relative_to(local_pipelines_root)
        return True
    except Exception:
        return False


def _normalize_pipeline_relpath(raw_pipeline: str) -> Path:
    raw = str(raw_pipeline or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="`pipeline` is required.")
    path = Path(raw).expanduser()
    if not path.is_absolute():
        parts = list(path.parts)
        if parts and str(parts[0]).lower() == "pipelines":
            path = Path(*parts[1:]) if len(parts) > 1 else Path("")
    if path.suffix.lower() not in {".yml", ".yaml"}:
        path = path.with_suffix(".yml")
    return path


def _builder_project_context(
    *,
    project_id: Optional[str],
    projects_config: Optional[str],
) -> tuple[Optional[str], dict[str, Any], Optional[Path]]:
    raw_cfg = str(projects_config or "").strip()
    cfg_path = Path(raw_cfg).expanduser() if raw_cfg else None
    try:
        resolved = resolve_projects_config_path(cfg_path)
    except ProjectConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    pid = normalize_project_id(project_id)
    if not pid:
        return None, {}, resolved
    try:
        project_vars = load_project_vars(project_id=pid, projects_config_path=resolved)
    except ProjectConfigError as exc:
        raise HTTPException(status_code=400, detail=f"Projects config error: {exc}") from exc
    return pid, project_vars, resolved


def _normalize_tree_label(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "repo"
    text = text.replace("\\", "/").strip("/")
    return text or "repo"


def _builder_pipeline_source_views(
    *,
    project_vars: dict[str, Any],
    repo_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    views: list[dict[str, Any]] = []
    warnings: list[str] = []
    sources = pipeline_asset_sources_from_project_vars(project_vars)
    if not sources:
        return views, warnings

    raw_sources = project_vars.get("pipeline_asset_sources")
    raw_list = raw_sources if isinstance(raw_sources, list) else []
    legacy_key = str(project_vars.get("pipeline_assets_project_key") or "").strip()
    cache_root = (repo_root / ".pipeline_assets_cache").resolve()
    used_labels: dict[str, int] = {}

    for idx, src in enumerate(sources):
        label = ""
        if len(sources) == 1 and legacy_key:
            label = legacy_key
        elif idx < len(raw_list):
            raw = raw_list[idx]
            if isinstance(raw, dict):
                label = str(raw.get("project_key") or raw.get("key") or raw.get("name") or "").strip()
        if not label:
            label = infer_repo_name(src.repo_url)
        label = _normalize_tree_label(label)
        seen = used_labels.get(label, 0) + 1
        used_labels[label] = seen
        if seen > 1:
            label = f"{label}-{seen}"

        try:
            repo_dir = sync_pipeline_asset_source(src, cache_root=cache_root, repo_root=repo_root)
        except PipelineAssetError as exc:
            warnings.append(f"{label}: {exc}")
            continue
        root = (repo_dir / src.pipelines_dir).resolve()
        if not root.exists() or not root.is_dir():
            warnings.append(f"{label}: pipelines dir not found: {root}")
            continue
        views.append({"label": label, "pipelines_root": root, "repo_root": repo_dir})
    return views, warnings


def _normalize_dag_pipeline_ref(value: str) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    if not raw:
        return ""
    while raw.startswith("./"):
        raw = raw[2:]
    if raw.lower().startswith("pipelines/"):
        raw = raw[len("pipelines/") :]
    path = Path(raw)
    if path.suffix.lower() not in {".yml", ".yaml"}:
        path = path.with_suffix(".yml")
    return path.as_posix()


def _collect_project_pipeline_graph(
    *,
    project_id: str,
    project_vars: dict[str, Any],
    repo_root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    views, warnings = _builder_pipeline_source_views(project_vars=project_vars, repo_root=repo_root)
    source_count = len(views)
    docs: list[dict[str, Any]] = []
    if views:
        for view in views:
            label = str(view["label"])
            root = Path(view["pipelines_root"]).resolve()
            for pat in ("*.yml", "*.yaml"):
                for p in root.rglob(pat):
                    if not p.is_file():
                        continue
                    rel = p.relative_to(root).as_posix()
                    node_id = f"{label}/{rel}" if source_count > 1 else rel
                    docs.append({"id": node_id, "pipeline": rel, "source": label, "path": p})
    else:
        local_root = (repo_root / "pipelines").resolve()
        for pat in ("*.yml", "*.yaml"):
            for p in local_root.rglob(pat):
                if not p.is_file():
                    continue
                rel = p.relative_to(local_root).as_posix()
                docs.append({"id": rel, "pipeline": rel, "source": "local", "path": p})
    docs.sort(key=lambda x: str(x.get("id") or "").lower())

    by_id: dict[str, dict[str, Any]] = {str(d["id"]): d for d in docs}
    by_rel_unique: dict[str, Optional[str]] = {}
    for d in docs:
        rel = str(d["pipeline"])
        if rel in by_rel_unique:
            by_rel_unique[rel] = None
        else:
            by_rel_unique[rel] = str(d["id"])

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    latest_run_cache: dict[str, Optional[dict[str, Any]]] = {}

    def _latest_run_for_pipeline(rel: str) -> Optional[dict[str, Any]]:
        if rel in latest_run_cache:
            return latest_run_cache[rel]
        try:
            rows = fetch_pipeline_runs(rel, limit=1, project_id=project_id)
        except WebQueryError as exc:
            warnings.append(str(exc))
            latest_run_cache[rel] = None
            return None
        latest_run_cache[rel] = rows[0] if rows else None
        return latest_run_cache[rel]

    import yaml

    def _parse_dt(value: Any) -> Optional[datetime]:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            return None

    for d in docs:
        node_id = str(d["id"])
        rel = str(d["pipeline"])
        source = str(d["source"])
        run = _latest_run_for_pipeline(rel)
        status = str((run or {}).get("status") or "not-run").strip().lower() or "not-run"
        node = {
            "id": node_id,
            "pipeline": rel,
            "source": source,
            "label": node_id,
            "exists": True,
            "status": status,
            "run_id": (run or {}).get("run_id"),
            "started_at": (run or {}).get("started_at"),
            "path": str(d["path"]),
            "parse_error": None,
            "stale": False,
            "stale_dependencies": [],
        }
        nodes[node_id] = node
        requires: list[str] = []
        try:
            raw = yaml.safe_load(Path(d["path"]).read_text(encoding="utf-8")) or {}
            reqs = raw.get("requires_pipelines", []) or []
            if isinstance(reqs, list):
                requires = [str(x or "").strip() for x in reqs if str(x or "").strip()]
            else:
                node["parse_error"] = "`requires_pipelines` must be a list"
                warnings.append(f"{node_id}: invalid requires_pipelines type")
        except Exception as exc:  # noqa: BLE001
            node["parse_error"] = str(exc)
            warnings.append(f"{node_id}: parse failed: {exc}")

        for req_raw in requires:
            req_rel = _normalize_dag_pipeline_ref(req_raw)
            if not req_rel:
                continue
            target_id = ""
            if source_count > 1:
                same_source_id = f"{source}/{req_rel}"
                if same_source_id in by_id:
                    target_id = same_source_id
            if not target_id:
                candidate = by_rel_unique.get(req_rel)
                if candidate:
                    target_id = candidate
            if not target_id and req_rel in by_id:
                target_id = req_rel
            if not target_id:
                target_id = f"missing:{req_rel}"
                if target_id not in nodes:
                    run2 = _latest_run_for_pipeline(req_rel)
                    status2 = str((run2 or {}).get("status") or "missing").strip().lower() or "missing"
                    nodes[target_id] = {
                        "id": target_id,
                        "pipeline": req_rel,
                        "source": None,
                        "label": req_rel,
                        "exists": False,
                        "status": status2 if run2 else "missing",
                        "run_id": (run2 or {}).get("run_id"),
                        "started_at": (run2 or {}).get("started_at"),
                        "path": None,
                        "parse_error": "dependency not found in project pipeline sources",
                    }
            edges.append({"from": target_id, "to": node_id, "missing": target_id.startswith("missing:"), "ref": req_rel})

    # Mark downstream nodes stale when a dependency has a newer run timestamp.
    incoming: dict[str, list[str]] = {}
    for e in edges:
        to_id = str(e.get("to") or "")
        from_id = str(e.get("from") or "")
        if not to_id or not from_id:
            continue
        incoming.setdefault(to_id, []).append(from_id)
    for node_id, node in nodes.items():
        node_run_at = _parse_dt(node.get("started_at"))
        if node_run_at is None:
            continue
        stale_from: list[str] = []
        for dep_id in incoming.get(node_id, []):
            dep = nodes.get(dep_id) or {}
            dep_run_at = _parse_dt(dep.get("started_at"))
            if dep_run_at is None:
                continue
            if dep_run_at > node_run_at:
                stale_from.append(dep_id)
        if stale_from:
            node["stale"] = True
            node["stale_dependencies"] = stale_from

    order = ["running", "queued", "failed", "cancel_requested", "cancelled", "succeeded", "not-run", "missing"]
    rank = {k: i for i, k in enumerate(order)}
    nodes_list = sorted(
        nodes.values(),
        key=lambda n: (rank.get(str(n.get("status") or "").lower(), 999), str(n.get("label") or "").lower()),
    )
    return nodes_list, edges, warnings


def _infer_external_pipeline_remote_hint(
    *,
    pipeline_path: Path,
    project_vars: dict[str, Any],
    repo_root: Path,
) -> Optional[str]:
    resolved = Path(pipeline_path).resolve()
    views, _warnings = _builder_pipeline_source_views(project_vars=project_vars, repo_root=repo_root)
    for view in views:
        root = Path(view.get("pipelines_root") or "").resolve()
        try:
            rel = resolved.relative_to(root)
        except ValueError:
            continue
        return (Path("pipelines") / rel).as_posix()
    return None


def _resolve_project_writable_pipeline_path(
    *,
    pipeline: str,
    project_id: Optional[str],
    projects_config: Optional[str],
    pipeline_source: Optional[str],
) -> Path:
    repo_root = Path(".").resolve()
    pid, project_vars, _resolved_cfg = _builder_project_context(project_id=project_id, projects_config=projects_config)
    if not pid:
        return _resolve_repo_relative_pipeline_path(pipeline)

    views, _warnings = _builder_pipeline_source_views(project_vars=project_vars, repo_root=repo_root)
    if not views:
        return _resolve_repo_relative_pipeline_path(pipeline)

    view_by_label = {str(v["label"]): v for v in views}
    rel = _normalize_pipeline_relpath(pipeline)
    rel_parts = [p for p in rel.as_posix().split("/") if p]
    inline_source = rel_parts[0] if rel_parts and rel_parts[0] in view_by_label else ""
    if inline_source:
        rel = Path("/".join(rel_parts[1:])) if len(rel_parts) > 1 else Path("")
        if rel.suffix.lower() not in {".yml", ".yaml"}:
            rel = rel.with_suffix(".yml")

    requested_source = str(pipeline_source or "").strip() or inline_source
    target_view = None
    if requested_source:
        target_view = view_by_label.get(requested_source)
        if target_view is None:
            raise HTTPException(status_code=400, detail=f"Unknown pipeline_source '{requested_source}'.")
    elif len(views) == 1:
        target_view = views[0]
    else:
        existing_matches: list[dict[str, Any]] = []
        for view in views:
            root = Path(view["pipelines_root"]).resolve()
            cand = (root / rel).resolve()
            try:
                cand.relative_to(root)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=f"Invalid pipeline path '{pipeline}'.") from exc
            if cand.exists() and cand.is_file():
                existing_matches.append(view)
        if len(existing_matches) == 1:
            target_view = existing_matches[0]
        elif len(existing_matches) > 1:
            labels = [str(v["label"]) for v in existing_matches]
            raise HTTPException(
                status_code=409,
                detail=f"Ambiguous pipeline path '{pipeline}' across sources {labels}. Pass pipeline_source.",
            )
        else:
            labels = [str(v["label"]) for v in views]
            raise HTTPException(
                status_code=400,
                detail=f"Multiple project pipeline sources available {labels}. Pass pipeline_source.",
            )

    root = Path(target_view["pipelines_root"]).resolve()
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid pipeline path '{pipeline}'.") from exc
    if candidate.suffix.lower() not in {".yml", ".yaml"}:
        candidate = candidate.with_suffix(".yml")
    return candidate


def api_pipelines_create(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    req_project_id = normalize_project_id(str(payload.get("project_id") or "").strip() or None)
    pipeline_source = str(payload.get("pipeline_source") or "").strip() or None
    pipeline_path = _resolve_project_writable_pipeline_path(
        pipeline=str(payload.get("pipeline") or ""),
        project_id=req_project_id,
        projects_config=str(payload.get("projects_config") or "").strip() or None,
        pipeline_source=pipeline_source,
    )
    path_project_id = infer_project_id_from_pipeline_path(pipeline_path) if _is_local_repo_pipeline_path(pipeline_path) else None
    if req_project_id and path_project_id and req_project_id != path_project_id:
        raise HTTPException(
            status_code=400,
            detail=f"Requested project_id '{req_project_id}' does not match pipeline path project '{path_project_id}'.",
        )
    _require_project_access(scope, req_project_id or path_project_id)
    yaml_text = str(payload.get("yaml_text") or "")
    _ = _parse_pipeline_from_yaml_text(
        yaml_text,
        global_config_path=None,
        project_id=req_project_id or path_project_id,
        projects_config_path=Path(str(payload.get("projects_config") or "").strip()).expanduser()
        if str(payload.get("projects_config") or "").strip()
        else None,
        pipeline_hint=str(pipeline_path),
    )
    overwrite = _parse_bool(payload.get("overwrite"), default=False)
    if pipeline_path.exists() and not overwrite:
        raise HTTPException(status_code=409, detail=f"Pipeline already exists: {pipeline_path}")
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return {"pipeline": str(pipeline_path), "saved": True, "created": True}


def api_pipelines_update(request: Request, pipeline_id: str, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    scope = _resolve_user_scope(request)
    payload = payload or {}
    if "pipeline" in payload and str(payload.get("pipeline") or "").strip() not in {"", pipeline_id}:
        raise HTTPException(status_code=400, detail="Payload pipeline does not match URL pipeline_id.")
    req_project_id = normalize_project_id(str(payload.get("project_id") or "").strip() or None)
    pipeline_source = str(payload.get("pipeline_source") or "").strip() or None
    pipeline_path = _resolve_project_writable_pipeline_path(
        pipeline=pipeline_id,
        project_id=req_project_id,
        projects_config=str(payload.get("projects_config") or "").strip() or None,
        pipeline_source=pipeline_source,
    )
    path_project_id = infer_project_id_from_pipeline_path(pipeline_path) if _is_local_repo_pipeline_path(pipeline_path) else None
    if req_project_id and path_project_id and req_project_id != path_project_id:
        raise HTTPException(
            status_code=400,
            detail=f"Requested project_id '{req_project_id}' does not match pipeline path project '{path_project_id}'.",
        )
    _require_project_access(scope, req_project_id or path_project_id)
    if not pipeline_path.exists():
        raise HTTPException(status_code=404, detail=f"Pipeline file not found: {pipeline_path}")
    yaml_text = str(payload.get("yaml_text") or "")
    _ = _parse_pipeline_from_yaml_text(
        yaml_text,
        global_config_path=None,
        project_id=req_project_id or path_project_id,
        projects_config_path=Path(str(payload.get("projects_config") or "").strip()).expanduser()
        if str(payload.get("projects_config") or "").strip()
        else None,
        pipeline_hint=str(pipeline_path),
    )
    pipeline_path.parent.mkdir(parents=True, exist_ok=True)
    pipeline_path.write_text(yaml_text.strip() + "\n", encoding="utf-8")
    return {"pipeline": str(pipeline_path), "saved": True, "updated": True}


def api_builder_source(
    pipeline: str = Query(default=""),
    project_id: Optional[str] = Query(default=None),
    projects_config: Optional[str] = Query(default=None),
    pipeline_source: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_source(pipeline, project_id, projects_config, pipeline_source, _builder_handler_deps())


def api_builder_files(
    project_id: Optional[str] = Query(default=None),
    projects_config: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_files(project_id, projects_config, _builder_handler_deps())


def api_project_dag(
    request: Request,
    project_id: str,
    projects_config: Optional[str] = Query(default=None),
) -> dict:
    scope = _resolve_user_scope(request)
    repo_root = Path(".").resolve()
    pid, project_vars, resolved_projects_cfg = _builder_project_context(
        project_id=project_id,
        projects_config=projects_config,
    )
    if not pid:
        raise HTTPException(status_code=400, detail="`project_id` is required.")
    _require_project_access(scope, pid)
    nodes, edges, warnings = _collect_project_pipeline_graph(
        project_id=pid,
        project_vars=project_vars,
        repo_root=repo_root,
    )
    return {
        "project_id": pid,
        "projects_config": str(resolved_projects_cfg) if resolved_projects_cfg else None,
        "nodes": nodes,
        "edges": edges,
        "warnings": warnings,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def api_builder_plugins(
    global_config: Optional[str] = Query(default=None),
    plugins_dir: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_plugins(global_config, plugins_dir, _builder_handler_deps())


def api_plugins_stats(
    global_config: Optional[str] = Query(default=None),
    plugins_dir: Optional[str] = Query(default=None),
    environments_config: Optional[str] = Query(default=None),
    env: Optional[str] = Query(default=None),
    low_sample_multiplier: float = Query(default=1.5, ge=1.0, le=10.0),
    limit: int = Query(default=200, ge=10, le=5000),
) -> dict:
    global_config_path = Path(global_config).expanduser() if (global_config or "").strip() else None
    root = _resolve_builder_plugins_dir(global_config_path=global_config_path, plugins_dir=plugins_dir)
    if not root.exists() or not root.is_dir():
        raise HTTPException(status_code=400, detail=f"Plugins directory not found: {root}")

    environments_config_path = Path(environments_config).expanduser() if (environments_config or "").strip() else None
    env_name = str(env or "").strip() or None
    env_vars = _resolve_builder_env_vars(
        environments_config_path=environments_config_path,
        env_name=env_name,
    )
    max_cpu = env_vars.get("max_cpus_per_task")
    try:
        max_cpu = int(max_cpu) if max_cpu not in (None, "") else None
    except (TypeError, ValueError):
        max_cpu = None
    max_mem_gb = _parse_mem_text_gb(env_vars.get("max_mem"))
    max_wall_minutes = _parse_slurm_time_minutes(env_vars.get("max_time"))

    entries: list[dict[str, Any]] = []
    for f in sorted(root.rglob("*.py")):
        if f.name.startswith("_"):
            continue
        rel = f.relative_to(root).as_posix()
        try:
            pd = load_plugin(f)
        except PluginLoadError:
            entries.append(
                {
                    "path": rel,
                    "name": rel,
                    "version": "",
                    "description": "unloadable plugin",
                    "resources": {},
                    "stats": {},
                    "recommendation": {},
                }
            )
            continue

        plugin_name = str(pd.meta.name or "").strip()
        plugin_version = str(pd.meta.version or "").strip()
        stats = fetch_plugin_resource_stats(
            plugin_name=plugin_name,
            plugin_version=plugin_version,
            plugin_refs=[rel],
            executor="slurm",
            limit=limit,
        )
        rec_cpu = _estimate_from_stats(
            stats.get("cpu_cores_mean"),
            stats.get("cpu_cores_std"),
            stats.get("cpu_cores_samples", stats.get("samples")),
            low_sample_multiplier,
        )
        rec_mem_gb = _estimate_from_stats(
            stats.get("memory_gb_mean"),
            stats.get("memory_gb_std"),
            stats.get("memory_gb_samples", stats.get("samples")),
            low_sample_multiplier,
        )
        rec_wall = _estimate_from_stats(
            stats.get("wall_minutes_mean"),
            stats.get("wall_minutes_std"),
            stats.get("wall_minutes_samples", stats.get("samples")),
            low_sample_multiplier,
        )

        if max_cpu is not None and rec_cpu is not None:
            rec_cpu = min(rec_cpu, float(max_cpu))
        if max_mem_gb is not None and rec_mem_gb is not None:
            rec_mem_gb = min(rec_mem_gb, float(max_mem_gb))
        if max_wall_minutes is not None and rec_wall is not None:
            rec_wall = min(rec_wall, float(max_wall_minutes))

        recommendation = {
            "cpu_cores": rec_cpu,
            "memory_gb": rec_mem_gb,
            "wall_minutes": rec_wall,
            "samples": int(stats.get("samples", 0) or 0),
            "low_sample_multiplier": float(low_sample_multiplier),
        }
        entries.append(
            {
                "path": rel,
                "name": plugin_name or rel,
                "version": plugin_version,
                "description": pd.meta.description or "",
                "resources": pd.meta.resources or {},
                "stats": stats or {},
                "recommendation": recommendation,
            }
        )
    return {
        "plugins_dir": str(root),
        "env": env_name,
        "caps": {
            "max_cpus_per_task": max_cpu,
            "max_mem_gb": max_mem_gb,
            "max_wall_minutes": max_wall_minutes,
        },
        "plugins": entries,
    }


def api_builder_environments(
    environments_config: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_environments(environments_config, _builder_handler_deps())


def api_builder_projects(request: Request, projects_config: Optional[str] = Query(default=None)) -> dict:
    return web_builder_handlers.api_builder_projects(projects_config, _builder_handler_deps())


def api_builder_project_vars(
    request: Request,
    project_id: Optional[str] = Query(default=None),
    projects_config: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_project_vars(project_id, projects_config, _builder_handler_deps())


def api_builder_git_status(
    project_id: Optional[str] = Query(default=None),
    projects_config: Optional[str] = Query(default=None),
    pipeline_source: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_git_status(project_id, projects_config, pipeline_source, _builder_handler_deps())


def api_builder_git_main_check(
    project_id: Optional[str] = Query(default=None),
    projects_config: Optional[str] = Query(default=None),
    pipeline_source: Optional[str] = Query(default=None),
) -> dict:
    return web_builder_handlers.api_builder_git_main_check(project_id, projects_config, pipeline_source, _builder_handler_deps())


def api_builder_git_sync(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_git_sync(payload, _builder_handler_deps())


def _builder_handler_deps() -> dict[str, Any]:
    return {
        "resolve_global_vars": _resolve_global_vars,
        "resolve_builder_env_vars": _resolve_builder_env_vars,
        "resolve_builder_project_vars": _resolve_builder_project_vars,
        "resolve_builder_plugins_dir": _resolve_builder_plugins_dir,
        "raw_vars_dirs_from_yaml_text": _raw_vars_dirs_from_yaml_text,
        "parse_pipeline_from_yaml_text": _parse_pipeline_from_yaml_text,
        "build_builder_namespace": _build_builder_namespace,
        "build_web_request_solver": _build_web_request_solver,
        "builder_project_context": _builder_project_context,
        "builder_pipeline_source_views": _builder_pipeline_source_views,
        "normalize_pipeline_relpath": _normalize_pipeline_relpath,
        "builder_git_target_repo_root": _builder_git_target_repo_root,
        "git_repo_status": _git_repo_status,
        "builder_git_sync_repo_root_from_env": _builder_git_sync_repo_root_from_env,
        "git_main_health": _git_main_health,
        "builder_git_sync": _builder_git_sync,
        "normalize_project_id": normalize_project_id,
        "parse_bool": _parse_bool,
        "extract_unresolved_tokens": _extract_unresolved_tokens,
        "resolve_workdir_from_solver": _resolve_workdir_from_solver,
        "resolve_text_with_ctx_iterative": _resolve_text_with_ctx_iterative,
        "validate_pipeline_dir_contract": _validate_pipeline_dir_contract,
        "filter_builder_unresolved_issues": _filter_builder_unresolved_issues,
        "collect_unresolved_step_inputs": _collect_unresolved_step_inputs,
        "record_pipeline_validation": _record_pipeline_validation,
        "pipeline_to_builder_model_from_yaml": _pipeline_to_builder_model_from_yaml,
        "validate_draft_yaml": _validate_draft_yaml,
        "generate_pipeline_draft": generate_pipeline_draft,
        "resolve_execution_config_path": resolve_execution_config_path,
        "load_execution_config": load_execution_config,
        "ExecutionConfigError": ExecutionConfigError,
        "resolve_projects_config_path": resolve_projects_config_path,
        "load_project_vars": load_project_vars,
        "ProjectConfigError": ProjectConfigError,
        "load_plugin": load_plugin,
        "PluginLoadError": PluginLoadError,
        "AIPipelineError": AIPipelineError,
        "run_pipeline": run_pipeline,
        "HpccDirectExecutor": HpccDirectExecutor,
        "SlurmExecutor": SlurmExecutor,
        "resolve_project_writable_pipeline_path": _resolve_project_writable_pipeline_path,
        "infer_external_pipeline_remote_hint": _infer_external_pipeline_remote_hint,
        "tail_text_lines": _tail_text_lines,
        "last_non_empty_line": _last_non_empty_line,
        "upsert_run_context_snapshot": upsert_run_context_snapshot,
        "load_builder_session": _load_builder_session,
        "builder_step_session_prepare": _builder_step_session_prepare,
        "builder_step_session_load_context": _builder_step_session_load_context,
        "builder_step_session_save_context": _builder_step_session_save_context,
        "builder_step_session_record_result": _builder_step_session_record_result,
        "DEFAULT_RESOLVE_MAX_PASSES": DEFAULT_RESOLVE_MAX_PASSES,
    }


def _resolve_builder_runtime_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return web_builder_handlers.resolve_builder_runtime_from_payload(payload, _builder_handler_deps())


def api_builder_resolve_text(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_resolve_text(payload, _builder_handler_deps())


def api_builder_namespace(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_namespace(payload, _builder_handler_deps())


def api_builder_validate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_validate(payload, _builder_handler_deps())


def api_builder_generate(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_generate(payload, _builder_handler_deps())


def _execute_builder_step_test(payload: dict[str, Any]) -> dict[str, Any]:
    return web_builder_handlers.execute_builder_step_test(payload, _builder_handler_deps())


def api_builder_test_step(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return _execute_builder_step_test(dict(payload or {}))


def api_builder_test_step_start(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_test_step_start(payload, _builder_handler_deps())


def api_builder_test_step_status(test_id: str = Query(default="")) -> dict:
    return web_builder_handlers.api_builder_test_step_status(test_id)


def api_builder_test_step_stop(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_builder_handlers.api_builder_test_step_stop(payload)


def api_builder_sessions_list(
    pipeline: Optional[str] = Query(default=None),
    project_id: Optional[str] = Query(default=None),
    env: Optional[str] = Query(default=None),
) -> dict:
    return _builder_sessions_list(pipeline=pipeline, project_id=project_id, env=env)


def api_builder_sessions_create(payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    base_payload = dict(payload or {})
    source_run_id = str(base_payload.get("source_run_id") or "").strip()
    prepared = _builder_step_session_prepare(base_payload)
    if source_run_id:
        context_file = str((prepared.get("payload") or {}).get("context_file") or "").strip()
        if context_file:
            snapshot_ctx = load_latest_run_context_snapshot(source_run_id)
            if snapshot_ctx:
                _builder_step_session_save_context(context_file=context_file, context=snapshot_ctx)
    return {"session": prepared["session"]}


def api_builder_sessions_get(session_id: str) -> dict:
    return _builder_session_get(session_id)


def _payload_with_pipeline(payload: Optional[dict[str, Any]], pipeline_id: str) -> dict[str, Any]:
    out = dict(payload or {})
    supplied = str(out.get("pipeline") or "").strip()
    if supplied and supplied != pipeline_id:
        raise HTTPException(
            status_code=400,
            detail=f"Payload pipeline '{supplied}' does not match URL pipeline '{pipeline_id}'.",
        )
    out["pipeline"] = pipeline_id
    return out


def _web_base_target_solver() -> Optional[VariableSolver]:
    ctx = _WEB_RUNTIME_CONTEXT
    if ctx is None:
        return None
    try:
        return ctx.solver("target")
    except Exception:
        return None


def _build_web_request_solver(
    *,
    global_vars: dict[str, Any],
    env_vars: dict[str, Any],
    project_vars: Optional[dict[str, Any]] = None,
    commandline_vars: Optional[dict[str, Any]] = None,
    context_vars: Optional[dict[str, Any]] = None,
    pipeline: Optional[Pipeline] = None,
) -> VariableSolver:
    max_passes = resolve_max_passes_setting(global_vars=global_vars, env_vars=env_vars)
    base_solver = _web_base_target_solver()
    if base_solver is not None:
        solver = VariableSolver(max_passes=max_passes, initial=base_solver.context())
    else:
        solver = VariableSolver(max_passes=max_passes)
    solver.overlay("global", global_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("globals", global_vars or {}, add_namespace=True, add_flat=False)
    solver.overlay("env", env_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("project", project_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("commandline", commandline_vars or {}, add_namespace=True, add_flat=True)
    solver.overlay("context", context_vars or {}, add_namespace=True, add_flat=False)
    if pipeline is not None:
        solver.overlay("pipe", dict(getattr(pipeline, "vars", {}) or {}), add_namespace=True, add_flat=True)
        solver.overlay("dirs", dict(getattr(pipeline, "dirs", {}) or {}), add_namespace=True, add_flat=True)
    return solver


def api_action_validate(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_action_handlers.api_action_validate(request, payload, _action_handler_deps())


def api_action_run(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_action_handlers.api_action_run(request, payload, _action_handler_deps())


def api_pipeline_validate(
    request: Request,
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_validate(request, _payload_with_pipeline(payload, pipeline_id))


def api_pipeline_run(
    request: Request,
    pipeline_id: str,
    payload: Optional[dict[str, Any]] = Body(default=None),
) -> dict:
    return api_action_run(request, _payload_with_pipeline(payload, pipeline_id))


def api_run_files(run_id: str, request: Request) -> dict:
    return web_run_artifact_handlers.api_run_files(run_id, request, _run_artifact_handler_deps())


def api_run_file(run_id: str, request: Request, path: str = Query(default="")) -> dict:
    return web_run_artifact_handlers.api_run_file(run_id, request, path, _run_artifact_handler_deps())


def api_run_live_log(run_id: str, request: Request, limit: int = Query(default=200, ge=1, le=2000)) -> dict:
    return web_run_artifact_handlers.api_run_live_log(run_id, request, limit, _run_artifact_handler_deps())


def api_query_preview(request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_query_handlers.api_query_preview(request, payload, _query_handler_deps())


def api_stop_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_action_handlers.api_stop_run(run_id, request, payload, _action_handler_deps())


def api_resume_run(run_id: str, request: Request, payload: Optional[dict[str, Any]] = Body(default=None)) -> dict:
    return web_action_handlers.api_resume_run(run_id, request, payload, _action_handler_deps())

