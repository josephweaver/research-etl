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


__all__ = ["WebQueryError", "fetch_runs", "fetch_run_header", "fetch_run_detail"]
