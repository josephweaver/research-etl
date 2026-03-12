# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import psycopg
import yaml

from ..db import get_database_url
from ..projects import normalize_project_id


class QueryWorkspaceError(RuntimeError):
    """Raised when query workspace configuration cannot be loaded or saved."""


def _normalize_workspace_id(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if text in {"project", "user"}:
        return text
    return "project"


def _workspace_scope_key(*, scope_type: str, user_id: Optional[str]) -> str:
    if scope_type == "user":
        return str(user_id or "").strip() or "unknown"
    return ""


def resolve_repo_workspace_config_path(
    *,
    project_id: str,
    project_vars: Dict[str, Any],
    repo_root: Path,
) -> Path:
    raw_explicit = str(project_vars.get("duckdb_workspace_config_path") or "").strip()
    if raw_explicit:
        explicit_path = Path(raw_explicit).expanduser()
        if not explicit_path.is_absolute():
            explicit_path = (repo_root / explicit_path).resolve()
        return explicit_path

    raw_assets_repo = str(project_vars.get("pipeline_assets_local_repo_path") or "").strip()
    if raw_assets_repo:
        assets_root = Path(raw_assets_repo).expanduser()
        if not assets_root.is_absolute():
            assets_root = (repo_root / assets_root).resolve()
        candidate = assets_root / "db" / "duckdb" / "workspace.yml"
        legacy_candidate = assets_root / "query" / "duckdb.workspace.yml"
        if legacy_candidate.exists() and not candidate.exists():
            return legacy_candidate
        return candidate

    pid = normalize_project_id(project_id) or "default"
    return (repo_root / "config" / "query_workspaces" / f"{pid}.yml").resolve()


def load_workspace_config_file(path: Path) -> Dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        raise QueryWorkspaceError(f"Invalid workspace config YAML: {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise QueryWorkspaceError(f"Workspace config must be a mapping: {path}")
    return dict(raw)


def deep_merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = dict(base or {})
    for key, value in dict(override or {}).items():
        prev = out.get(key)
        if isinstance(prev, dict) and isinstance(value, dict):
            out[key] = deep_merge_config(dict(prev), dict(value))
        else:
            out[key] = value
    return out


def _connect() -> psycopg.Connection:
    db_url = get_database_url()
    if not db_url:
        raise QueryWorkspaceError("ETL_DATABASE_URL is not configured.")
    try:
        return psycopg.connect(db_url)
    except Exception as exc:  # noqa: BLE001
        raise QueryWorkspaceError(f"Could not connect to database: {exc}") from exc


def load_workspace_overrides(
    *,
    project_id: str,
    user_id: Optional[str],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    pid = normalize_project_id(project_id)
    if not pid:
        raise QueryWorkspaceError("project_id is required.")
    uid = str(user_id or "").strip()
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT scope_type, scope_key, config_json
                    FROM etl_query_workspaces
                    WHERE project_id = %s
                      AND (
                        (scope_type = 'project' AND scope_key = '')
                        OR (scope_type = 'user' AND scope_key = %s)
                      )
                    """,
                    (pid, uid),
                )
                rows = cur.fetchall() or []
    except QueryWorkspaceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise QueryWorkspaceError(f"Failed to load query workspace overrides: {exc}") from exc

    project_cfg: Dict[str, Any] = {}
    user_cfg: Dict[str, Any] = {}
    for row in rows:
        scope_type = str(row[0] or "").strip().lower()
        config_json = row[2]
        parsed = config_json if isinstance(config_json, dict) else {}
        if scope_type == "project":
            project_cfg = dict(parsed)
        elif scope_type == "user":
            user_cfg = dict(parsed)
    return project_cfg, user_cfg


def upsert_workspace_override(
    *,
    project_id: str,
    user_id: Optional[str],
    scope_type: str,
    config: Dict[str, Any],
    updated_by: Optional[str] = None,
    source: Optional[str] = None,
) -> Dict[str, Any]:
    pid = normalize_project_id(project_id)
    if not pid:
        raise QueryWorkspaceError("project_id is required.")
    st = _normalize_workspace_id(scope_type)
    sk = _workspace_scope_key(scope_type=st, user_id=user_id)
    cfg = dict(config or {})
    if not isinstance(cfg, dict):
        raise QueryWorkspaceError("workspace config must be an object.")

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO etl_query_workspaces (
                        project_id, scope_type, scope_key, config_json, source, updated_by
                    ) VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (project_id, scope_type, scope_key)
                    DO UPDATE SET
                        config_json = EXCLUDED.config_json,
                        source = EXCLUDED.source,
                        updated_by = EXCLUDED.updated_by,
                        updated_at = NOW()
                    RETURNING
                        workspace_id, project_id, scope_type, scope_key,
                        config_json, source, updated_by, created_at, updated_at
                    """,
                    (
                        pid,
                        st,
                        sk,
                        json.dumps(cfg),
                        str(source or "").strip() or None,
                        str(updated_by or "").strip() or None,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
    except QueryWorkspaceError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise QueryWorkspaceError(f"Failed to save query workspace override: {exc}") from exc

    if not row:
        raise QueryWorkspaceError("Failed to save query workspace override.")
    return {
        "workspace_id": int(row[0]),
        "project_id": str(row[1] or ""),
        "scope_type": str(row[2] or ""),
        "scope_key": str(row[3] or ""),
        "config": dict(row[4] or {}),
        "source": row[5],
        "updated_by": row[6],
        "created_at": row[7].isoformat() if row[7] is not None else None,
        "updated_at": row[8].isoformat() if row[8] is not None else None,
    }


__all__ = [
    "QueryWorkspaceError",
    "resolve_repo_workspace_config_path",
    "load_workspace_config_file",
    "deep_merge_config",
    "load_workspace_overrides",
    "upsert_workspace_override",
]
