# research-etl
# Copyright (c) 2026 Joseph Weaver
# This file is part of the research-etl project and is licensed under the MIT License.
# You may not use this file except in compliance with the License.
# See https://github.com/josephweaver/research-etl for details.

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import psycopg

from .db import get_database_url


class WebQueryError(RuntimeError):
    """Raised when web UI database queries fail."""


def _jsonify(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _connect() -> psycopg.Connection:
    db_url = get_database_url()
    if not db_url:
        raise WebQueryError("ETL_DATABASE_URL is not configured.")
    try:
        return psycopg.connect(db_url)
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Could not connect to database: {exc}") from exc


def fetch_runs(
    limit: int = 50,
    *,
    status: Optional[str] = None,
    executor: Optional[str] = None,
    q: Optional[str] = None,
    project_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_parts = []
                params: List[Any] = []
                if status:
                    where_parts.append("status = %s")
                    params.append(status)
                if executor:
                    where_parts.append("executor = %s")
                    params.append(executor)
                if q:
                    where_parts.append("(run_id ILIKE %s OR pipeline ILIKE %s)")
                    like = f"%{q}%"
                    params.extend([like, like])
                if project_id:
                    where_parts.append("project_id = %s")
                    params.append(project_id)
                where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
                cur.execute(
                    f"""
                    SELECT
                        run_id, pipeline, project_id, status, success, started_at, ended_at,
                        message, executor, artifact_dir
                    FROM etl_runs
                    {where_sql}
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch runs: {exc}") from exc

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "run_id": row[0],
                "pipeline": row[1],
                "project_id": row[2],
                "status": row[3],
                "success": bool(row[4]),
                "started_at": row[5].isoformat() if row[5] is not None else None,
                "ended_at": row[6].isoformat() if row[6] is not None else None,
                "message": row[7],
                "executor": row[8],
                "artifact_dir": row[9],
            }
        )
    return out


def fetch_pipelines(
    limit: int = 100,
    *,
    q: Optional[str] = None,
    project_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_sql = ""
                params: List[Any] = []
                if q:
                    where_sql = "WHERE pipeline ILIKE %s"
                    params.append(f"%{q}%")
                if project_id:
                    where_sql = f"{where_sql} {'AND' if where_sql else 'WHERE'} project_id = %s"
                    params.append(project_id)
                cur.execute(
                    f"""
                    WITH grouped AS (
                        SELECT
                            pipeline,
                            project_id,
                            COUNT(*) AS total_runs,
                            SUM(CASE WHEN status = 'failed' OR NOT success THEN 1 ELSE 0 END) AS failed_runs,
                            MAX(started_at) AS last_started_at
                        FROM etl_runs
                        {where_sql}
                        GROUP BY pipeline, project_id
                    )
                    SELECT
                        g.pipeline,
                        g.project_id,
                        g.total_runs,
                        g.failed_runs,
                        g.last_started_at,
                        r.status AS last_status,
                        r.executor AS last_executor
                    FROM grouped g
                    LEFT JOIN LATERAL (
                        SELECT status, executor
                        FROM etl_runs rr
                        WHERE rr.pipeline = g.pipeline
                          AND COALESCE(rr.project_id, '') = COALESCE(g.project_id, '')
                        ORDER BY rr.started_at DESC NULLS LAST, rr.run_id DESC
                        LIMIT 1
                    ) r ON TRUE
                    ORDER BY g.last_started_at DESC NULLS LAST, g.pipeline
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch pipelines: {exc}") from exc

    out: List[Dict[str, Any]] = []
    for row in rows:
        total_runs = int(row[2] or 0)
        failed_runs = int(row[3] or 0)
        failure_rate = (failed_runs / total_runs) if total_runs else 0.0
        out.append(
            {
                "pipeline": row[0],
                "project_id": row[1],
                "total_runs": total_runs,
                "failed_runs": failed_runs,
                "failure_rate": failure_rate,
                "last_started_at": row[4].isoformat() if row[4] is not None else None,
                "last_status": row[5],
                "last_executor": row[6],
            }
        )
    return out


def fetch_pipeline_detail(pipeline: str, *, project_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    pipeline = (pipeline or "").strip()
    if not pipeline:
        return None
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_project = ""
                params: list[Any] = [pipeline]
                if project_id:
                    where_project = "AND project_id = %s"
                    params.append(project_id)
                cur.execute(
                    f"""
                    WITH stats AS (
                        SELECT
                            pipeline,
                            MAX(project_id) AS project_id,
                            COUNT(*) AS total_runs,
                            SUM(CASE WHEN status = 'failed' OR NOT success THEN 1 ELSE 0 END) AS failed_runs,
                            MAX(started_at) AS last_started_at
                        FROM etl_runs
                        WHERE pipeline = %s {where_project}
                        GROUP BY pipeline
                    )
                    SELECT
                        s.pipeline,
                        s.project_id,
                        s.total_runs,
                        s.failed_runs,
                        s.last_started_at,
                        r.run_id,
                        r.project_id,
                        r.status,
                        r.executor,
                        r.started_at,
                        r.ended_at,
                        r.git_commit_sha,
                        r.git_branch,
                        r.git_tag,
                        r.git_is_dirty,
                        r.cli_command,
                        r.pipeline_checksum,
                        r.global_config_checksum,
                        r.execution_config_checksum,
                        r.plugin_checksums_json
                    FROM stats s
                    LEFT JOIN LATERAL (
                        SELECT
                            run_id, status, executor, started_at, ended_at,
                            git_commit_sha, git_branch, git_tag, git_is_dirty, cli_command,
                            pipeline_checksum, global_config_checksum, execution_config_checksum, plugin_checksums_json
                        FROM etl_runs rr
                        WHERE rr.pipeline = s.pipeline
                          AND COALESCE(rr.project_id, '') = COALESCE(s.project_id, '')
                        ORDER BY rr.started_at DESC NULLS LAST, rr.run_id DESC
                        LIMIT 1
                    ) r ON TRUE
                    """,
                    tuple(params),
                )
                row = cur.fetchone()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch pipeline detail: {exc}") from exc

    if not row:
        return None
    total_runs = int(row[2] or 0)
    failed_runs = int(row[3] or 0)
    failure_rate = (failed_runs / total_runs) if total_runs else 0.0
    return {
        "pipeline": row[0],
        "project_id": row[1],
        "total_runs": total_runs,
        "failed_runs": failed_runs,
        "failure_rate": failure_rate,
        "last_started_at": row[4].isoformat() if row[4] is not None else None,
        "latest_run": {
            "run_id": row[5],
            "project_id": row[6],
            "status": row[7],
            "executor": row[8],
            "started_at": row[9].isoformat() if row[9] is not None else None,
            "ended_at": row[10].isoformat() if row[10] is not None else None,
        },
        "latest_provenance": {
            "git_commit_sha": row[11],
            "git_branch": row[12],
            "git_tag": row[13],
            "git_is_dirty": row[14],
            "cli_command": row[15],
            "pipeline_checksum": row[16],
            "global_config_checksum": row[17],
            "execution_config_checksum": row[18],
            "plugin_checksums_json": _jsonify(row[19]),
        },
    }


def fetch_pipeline_runs(
    pipeline: str,
    limit: int = 50,
    *,
    status: Optional[str] = None,
    executor: Optional[str] = None,
    project_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pipeline = (pipeline or "").strip()
    if not pipeline:
        return []
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_parts = ["pipeline = %s"]
                params: List[Any] = [pipeline]
                if status:
                    where_parts.append("status = %s")
                    params.append(status)
                if executor:
                    where_parts.append("executor = %s")
                    params.append(executor)
                if project_id:
                    where_parts.append("project_id = %s")
                    params.append(project_id)
                cur.execute(
                    f"""
                    SELECT
                        run_id, pipeline, project_id, status, success, started_at, ended_at,
                        message, executor, artifact_dir
                    FROM etl_runs
                    WHERE {' AND '.join(where_parts)}
                    ORDER BY started_at DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch pipeline runs: {exc}") from exc

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "run_id": row[0],
                "pipeline": row[1],
                "project_id": row[2],
                "status": row[3],
                "success": bool(row[4]),
                "started_at": row[5].isoformat() if row[5] is not None else None,
                "ended_at": row[6].isoformat() if row[6] is not None else None,
                "message": row[7],
                "executor": row[8],
                "artifact_dir": row[9],
            }
        )
    return out


def fetch_pipeline_validations(
    pipeline: str,
    limit: int = 50,
    *,
    project_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    pipeline = (pipeline or "").strip()
    if not pipeline:
        return []
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_sql = "WHERE pipeline = %s"
                params: list[Any] = [pipeline]
                if project_id:
                    where_sql += " AND project_id = %s"
                    params.append(project_id)
                cur.execute(
                    f"""
                    SELECT
                        validation_id, pipeline, project_id, valid, step_count, step_names_json, error, source, requested_at
                    FROM etl_pipeline_validations
                    {where_sql}
                    ORDER BY requested_at DESC, validation_id DESC
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch pipeline validations: {exc}") from exc

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "validation_id": int(row[0]),
                "pipeline": row[1],
                "project_id": row[2],
                "valid": bool(row[3]),
                "step_count": int(row[4] or 0),
                "step_names": _jsonify(row[5]) or [],
                "error": row[6],
                "source": row[7],
                "requested_at": row[8].isoformat() if row[8] is not None else None,
            }
        )
    return out


def fetch_datasets(
    limit: int = 100,
    *,
    q: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                where_sql = ""
                params: list[Any] = []
                if q:
                    where_sql = "WHERE (d.dataset_id ILIKE %s OR COALESCE(d.owner_user, '') ILIKE %s)"
                    like = f"%{q}%"
                    params.extend([like, like])
                cur.execute(
                    f"""
                    SELECT
                        d.dataset_id,
                        d.data_class,
                        d.owner_user,
                        d.status,
                        d.created_at,
                        d.updated_at,
                        COALESCE(v.version_count, 0) AS version_count,
                        lv.version_label AS latest_version,
                        COALESCE(dr.dictionary_repo_count, 0) AS dictionary_repo_count
                    FROM etl_datasets d
                    LEFT JOIN (
                        SELECT dataset_id, COUNT(*) AS version_count
                        FROM etl_dataset_versions
                        GROUP BY dataset_id
                    ) v ON v.dataset_id = d.dataset_id
                    LEFT JOIN LATERAL (
                        SELECT version_label
                        FROM etl_dataset_versions vv
                        WHERE vv.dataset_id = d.dataset_id
                        ORDER BY vv.created_at DESC, vv.dataset_version_id DESC
                        LIMIT 1
                    ) lv ON TRUE
                    LEFT JOIN (
                        SELECT dataset_id, COUNT(DISTINCT repo_id) AS dictionary_repo_count
                        FROM etl_dataset_dictionary_entries
                        GROUP BY dataset_id
                    ) dr ON dr.dataset_id = d.dataset_id
                    {where_sql}
                    ORDER BY d.dataset_id
                    LIMIT %s
                    """,
                    (*params, limit),
                )
                rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch datasets: {exc}") from exc

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "dataset_id": row[0],
                "data_class": row[1],
                "owner_user": row[2],
                "status": row[3],
                "created_at": row[4].isoformat() if row[4] is not None else None,
                "updated_at": row[5].isoformat() if row[5] is not None else None,
                "version_count": int(row[6] or 0),
                "latest_version": row[7],
                "dictionary_repo_count": int(row[8] or 0),
            }
        )
    return out


def fetch_dataset_detail(dataset_id: str) -> Optional[Dict[str, Any]]:
    dataset_id = (dataset_id or "").strip()
    if not dataset_id:
        return None
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        dataset_id, data_class, owner_user, status, created_at, updated_at
                    FROM etl_datasets
                    WHERE dataset_id = %s
                    """,
                    (dataset_id,),
                )
                base = cur.fetchone()
                if not base:
                    return None

                cur.execute(
                    """
                    SELECT
                        dataset_version_id, version_label, is_immutable, schema_hash, created_by_run_id, created_at
                    FROM etl_dataset_versions
                    WHERE dataset_id = %s
                    ORDER BY created_at DESC, dataset_version_id DESC
                    """,
                    (dataset_id,),
                )
                version_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        l.dataset_version_id,
                        v.version_label,
                        l.environment,
                        l.location_type,
                        l.uri,
                        l.is_canonical,
                        l.checksum,
                        l.size_bytes,
                        l.created_at
                    FROM etl_dataset_locations l
                    JOIN etl_dataset_versions v
                      ON v.dataset_version_id = l.dataset_version_id
                    WHERE v.dataset_id = %s
                    ORDER BY v.created_at DESC, l.created_at DESC, l.dataset_location_id DESC
                    """,
                    (dataset_id,),
                )
                location_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT
                        dr.repo_key,
                        dr.provider,
                        dr.owner,
                        dr.repo_name,
                        dr.default_branch,
                        dr.local_path,
                        de.file_path,
                        de.file_sha,
                        de.pr_url,
                        de.pr_number,
                        de.review_status,
                        de.last_synced_at,
                        de.updated_at
                    FROM etl_dataset_dictionary_entries de
                    JOIN etl_dictionary_repos dr
                      ON dr.repo_id = de.repo_id
                    WHERE de.dataset_id = %s
                    ORDER BY dr.repo_key, de.file_path
                    """,
                    (dataset_id,),
                )
                dictionary_rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch dataset detail: {exc}") from exc

    versions: list[dict[str, Any]] = []
    for row in version_rows:
        versions.append(
            {
                "dataset_version_id": int(row[0]),
                "version_label": row[1],
                "is_immutable": bool(row[2]),
                "schema_hash": row[3],
                "created_by_run_id": row[4],
                "created_at": row[5].isoformat() if row[5] is not None else None,
            }
        )

    locations: list[dict[str, Any]] = []
    for row in location_rows:
        locations.append(
            {
                "dataset_version_id": int(row[0]),
                "version_label": row[1],
                "environment": row[2],
                "location_type": row[3],
                "uri": row[4],
                "is_canonical": bool(row[5]),
                "checksum": row[6],
                "size_bytes": int(row[7]) if row[7] is not None else None,
                "created_at": row[8].isoformat() if row[8] is not None else None,
            }
        )

    dictionary_entries: list[dict[str, Any]] = []
    for row in dictionary_rows:
        dictionary_entries.append(
            {
                "repo_key": row[0],
                "provider": row[1],
                "owner": row[2],
                "repo_name": row[3],
                "default_branch": row[4],
                "local_path": row[5],
                "file_path": row[6],
                "file_sha": row[7],
                "pr_url": row[8],
                "pr_number": int(row[9]) if row[9] is not None else None,
                "review_status": row[10],
                "last_synced_at": row[11].isoformat() if row[11] is not None else None,
                "updated_at": row[12].isoformat() if row[12] is not None else None,
            }
        )

    return {
        "dataset_id": base[0],
        "data_class": base[1],
        "owner_user": base[2],
        "status": base[3],
        "created_at": base[4].isoformat() if base[4] is not None else None,
        "updated_at": base[5].isoformat() if base[5] is not None else None,
        "versions": versions,
        "locations": locations,
        "dictionary_entries": dictionary_entries,
    }


def fetch_run_header(run_id: str) -> Optional[Dict[str, Any]]:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, pipeline, project_id, executor, status, artifact_dir
                    FROM etl_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "run_id": row[0],
                    "pipeline": row[1],
                    "project_id": row[2],
                    "executor": row[3],
                    "status": row[4],
                    "artifact_dir": row[5],
                }
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch run header: {exc}") from exc


def fetch_run_detail(run_id: str) -> Optional[Dict[str, Any]]:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        run_id, pipeline, project_id, status, success, started_at, ended_at, message, executor, artifact_dir,
                        git_commit_sha, git_branch, git_tag, git_is_dirty, cli_command,
                        pipeline_checksum, global_config_checksum, execution_config_checksum, plugin_checksums_json
                    FROM etl_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                run_row = cur.fetchone()
                if not run_row:
                    return None

                cur.execute(
                    """
                    SELECT step_name, script, success, skipped, error, outputs_json
                    FROM etl_run_steps
                    WHERE run_id = %s
                    ORDER BY step_name
                    """,
                    (run_id,),
                )
                step_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT step_name, attempt_no, success, skipped, error, outputs_json, started_at, ended_at
                    FROM etl_run_step_attempts
                    WHERE run_id = %s
                    ORDER BY step_name, attempt_no
                    """,
                    (run_id,),
                )
                attempt_rows = cur.fetchall()

                cur.execute(
                    """
                    SELECT event_id, event_type, event_time, details_json
                    FROM etl_run_events
                    WHERE run_id = %s
                    ORDER BY event_id
                    """,
                    (run_id,),
                )
                event_rows = cur.fetchall()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch run details: {exc}") from exc

    steps = []
    for row in step_rows:
        steps.append(
            {
                "step_name": row[0],
                "script": row[1],
                "success": bool(row[2]),
                "skipped": bool(row[3]),
                "error": row[4],
                "outputs_json": _jsonify(row[5]),
            }
        )

    attempts = []
    for row in attempt_rows:
        attempts.append(
            {
                "step_name": row[0],
                "attempt_no": int(row[1]),
                "success": bool(row[2]),
                "skipped": bool(row[3]),
                "error": row[4],
                "outputs_json": _jsonify(row[5]),
                "started_at": row[6].isoformat() if row[6] is not None else None,
                "ended_at": row[7].isoformat() if row[7] is not None else None,
            }
        )

    events = []
    for row in event_rows:
        events.append(
            {
                "event_id": int(row[0]),
                "event_type": row[1],
                "event_time": row[2].isoformat() if row[2] is not None else None,
                "details_json": _jsonify(row[3]),
            }
        )

    return {
        "run_id": run_row[0],
        "pipeline": run_row[1],
        "project_id": run_row[2],
        "status": run_row[3],
        "success": bool(run_row[4]),
        "started_at": run_row[5].isoformat() if run_row[5] is not None else None,
        "ended_at": run_row[6].isoformat() if run_row[6] is not None else None,
        "message": run_row[7],
        "executor": run_row[8],
        "artifact_dir": run_row[9],
        "provenance": {
            "git_commit_sha": run_row[10],
            "git_branch": run_row[11],
            "git_tag": run_row[12],
            "git_is_dirty": run_row[13],
            "cli_command": run_row[14],
            "pipeline_checksum": run_row[15],
            "global_config_checksum": run_row[16],
            "execution_config_checksum": run_row[17],
            "plugin_checksums_json": _jsonify(run_row[18]),
        },
        "steps": steps,
        "attempts": attempts,
        "events": events,
    }


__all__ = [
    "WebQueryError",
    "fetch_runs",
    "fetch_pipelines",
    "fetch_pipeline_detail",
    "fetch_pipeline_runs",
    "fetch_pipeline_validations",
    "fetch_datasets",
    "fetch_dataset_detail",
    "fetch_run_header",
    "fetch_run_detail",
]
