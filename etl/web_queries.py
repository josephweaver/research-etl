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
                where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
                cur.execute(
                    f"""
                    SELECT
                        run_id, pipeline, status, success, started_at, ended_at,
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
                "status": row[2],
                "success": bool(row[3]),
                "started_at": row[4].isoformat() if row[4] is not None else None,
                "ended_at": row[5].isoformat() if row[5] is not None else None,
                "message": row[6],
                "executor": row[7],
                "artifact_dir": row[8],
            }
        )
    return out


def fetch_pipelines(
    limit: int = 100,
    *,
    q: Optional[str] = None,
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
                cur.execute(
                    f"""
                    WITH grouped AS (
                        SELECT
                            pipeline,
                            COUNT(*) AS total_runs,
                            SUM(CASE WHEN status = 'failed' OR NOT success THEN 1 ELSE 0 END) AS failed_runs,
                            MAX(started_at) AS last_started_at
                        FROM etl_runs
                        {where_sql}
                        GROUP BY pipeline
                    )
                    SELECT
                        g.pipeline,
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
        total_runs = int(row[1] or 0)
        failed_runs = int(row[2] or 0)
        failure_rate = (failed_runs / total_runs) if total_runs else 0.0
        out.append(
            {
                "pipeline": row[0],
                "total_runs": total_runs,
                "failed_runs": failed_runs,
                "failure_rate": failure_rate,
                "last_started_at": row[3].isoformat() if row[3] is not None else None,
                "last_status": row[4],
                "last_executor": row[5],
            }
        )
    return out


def fetch_pipeline_detail(pipeline: str) -> Optional[Dict[str, Any]]:
    pipeline = (pipeline or "").strip()
    if not pipeline:
        return None
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH stats AS (
                        SELECT
                            pipeline,
                            COUNT(*) AS total_runs,
                            SUM(CASE WHEN status = 'failed' OR NOT success THEN 1 ELSE 0 END) AS failed_runs,
                            MAX(started_at) AS last_started_at
                        FROM etl_runs
                        WHERE pipeline = %s
                        GROUP BY pipeline
                    )
                    SELECT
                        s.pipeline,
                        s.total_runs,
                        s.failed_runs,
                        s.last_started_at,
                        r.run_id,
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
                        ORDER BY rr.started_at DESC NULLS LAST, rr.run_id DESC
                        LIMIT 1
                    ) r ON TRUE
                    """,
                    (pipeline,),
                )
                row = cur.fetchone()
    except WebQueryError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise WebQueryError(f"Failed to fetch pipeline detail: {exc}") from exc

    if not row:
        return None
    total_runs = int(row[1] or 0)
    failed_runs = int(row[2] or 0)
    failure_rate = (failed_runs / total_runs) if total_runs else 0.0
    return {
        "pipeline": row[0],
        "total_runs": total_runs,
        "failed_runs": failed_runs,
        "failure_rate": failure_rate,
        "last_started_at": row[3].isoformat() if row[3] is not None else None,
        "latest_run": {
            "run_id": row[4],
            "status": row[5],
            "executor": row[6],
            "started_at": row[7].isoformat() if row[7] is not None else None,
            "ended_at": row[8].isoformat() if row[8] is not None else None,
        },
        "latest_provenance": {
            "git_commit_sha": row[9],
            "git_branch": row[10],
            "git_tag": row[11],
            "git_is_dirty": row[12],
            "cli_command": row[13],
            "pipeline_checksum": row[14],
            "global_config_checksum": row[15],
            "execution_config_checksum": row[16],
            "plugin_checksums_json": _jsonify(row[17]),
        },
    }


def fetch_pipeline_runs(
    pipeline: str,
    limit: int = 50,
    *,
    status: Optional[str] = None,
    executor: Optional[str] = None,
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
                cur.execute(
                    f"""
                    SELECT
                        run_id, pipeline, status, success, started_at, ended_at,
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
                "status": row[2],
                "success": bool(row[3]),
                "started_at": row[4].isoformat() if row[4] is not None else None,
                "ended_at": row[5].isoformat() if row[5] is not None else None,
                "message": row[6],
                "executor": row[7],
                "artifact_dir": row[8],
            }
        )
    return out


def fetch_pipeline_validations(
    pipeline: str,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    pipeline = (pipeline or "").strip()
    if not pipeline:
        return []
    limit = max(1, min(int(limit), 500))
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        validation_id, pipeline, valid, step_count, step_names_json, error, source, requested_at
                    FROM etl_pipeline_validations
                    WHERE pipeline = %s
                    ORDER BY requested_at DESC, validation_id DESC
                    LIMIT %s
                    """,
                    (pipeline, limit),
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
                "valid": bool(row[2]),
                "step_count": int(row[3] or 0),
                "step_names": _jsonify(row[4]) or [],
                "error": row[5],
                "source": row[6],
                "requested_at": row[7].isoformat() if row[7] is not None else None,
            }
        )
    return out


def fetch_run_header(run_id: str) -> Optional[Dict[str, Any]]:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT run_id, pipeline, executor, status, artifact_dir
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
                    "executor": row[2],
                    "status": row[3],
                    "artifact_dir": row[4],
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
                        run_id, pipeline, status, success, started_at, ended_at, message, executor, artifact_dir,
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
        "status": run_row[2],
        "success": bool(run_row[3]),
        "started_at": run_row[4].isoformat() if run_row[4] is not None else None,
        "ended_at": run_row[5].isoformat() if run_row[5] is not None else None,
        "message": run_row[6],
        "executor": run_row[7],
        "artifact_dir": run_row[8],
        "provenance": {
            "git_commit_sha": run_row[9],
            "git_branch": run_row[10],
            "git_tag": run_row[11],
            "git_is_dirty": run_row[12],
            "cli_command": run_row[13],
            "pipeline_checksum": run_row[14],
            "global_config_checksum": run_row[15],
            "execution_config_checksum": run_row[16],
            "plugin_checksums_json": _jsonify(run_row[17]),
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
    "fetch_run_header",
    "fetch_run_detail",
]
