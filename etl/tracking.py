"""
Run tracking persistence (JSONL-based).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg

from .db import get_database_url
from .runner import RunResult, StepResult


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


@dataclass
class RunRecord:
    run_id: str
    pipeline: str
    success: bool
    status: str
    started_at: str
    ended_at: str
    message: str = ""
    steps: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class RunStepState:
    step_name: str
    success: bool
    skipped: bool
    outputs: Dict[str, Any] = field(default_factory=dict)


def _step_to_dict(step_result: StepResult) -> Dict[str, Any]:
    return {
        "name": step_result.step.name,
        "script": step_result.step.script,
        "success": step_result.success,
        "skipped": step_result.skipped,
        "error": step_result.error,
        "outputs": step_result.outputs,
        "attempt_no": step_result.attempt_no,
        "attempts": step_result.attempts,
    }


def _upsert_run_db(
    rec: RunRecord,
    *,
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
) -> None:
    db_url = get_database_url()
    if not db_url:
        return

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_runs (
                    run_id, pipeline, success, status, started_at, ended_at, message, executor, artifact_dir
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id)
                DO UPDATE SET
                    pipeline = EXCLUDED.pipeline,
                    success = EXCLUDED.success,
                    status = EXCLUDED.status,
                    started_at = LEAST(etl_runs.started_at, EXCLUDED.started_at),
                    ended_at = GREATEST(etl_runs.ended_at, EXCLUDED.ended_at),
                    message = EXCLUDED.message,
                    executor = EXCLUDED.executor,
                    artifact_dir = EXCLUDED.artifact_dir
                """,
                (
                    rec.run_id,
                    rec.pipeline,
                    rec.success,
                    rec.status,
                    rec.started_at,
                    rec.ended_at,
                    rec.message,
                    executor,
                    artifact_dir,
                ),
            )

            for step in rec.steps:
                outputs_json = json.dumps(step.get("outputs"), default=str)
                step_name = step.get("name")
                step_script = step.get("script")
                step_success = step.get("success", False)
                step_skipped = step.get("skipped", False)
                step_error = step.get("error")
                cur.execute(
                    """
                    INSERT INTO etl_run_steps (
                        run_id, step_name, script, success, skipped, error, outputs_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (run_id, step_name)
                    DO UPDATE SET
                        script = EXCLUDED.script,
                        success = EXCLUDED.success,
                        skipped = EXCLUDED.skipped,
                        error = EXCLUDED.error,
                        outputs_json = EXCLUDED.outputs_json
                    """,
                    (
                        rec.run_id,
                        step_name,
                        step_script,
                        step_success,
                        step_skipped,
                        step_error,
                        outputs_json,
                    ),
                )
                attempts = step.get("attempts") or []
                if attempts:
                    for att in attempts:
                        att_outputs_json = json.dumps(att.get("outputs") or {}, default=str)
                        cur.execute(
                            """
                            INSERT INTO etl_run_step_attempts (
                                run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at, updated_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, NOW())
                            ON CONFLICT (run_id, step_name, attempt_no)
                            DO UPDATE SET
                                script = EXCLUDED.script,
                                success = EXCLUDED.success,
                                skipped = EXCLUDED.skipped,
                                error = EXCLUDED.error,
                                outputs_json = EXCLUDED.outputs_json,
                                started_at = EXCLUDED.started_at,
                                ended_at = EXCLUDED.ended_at,
                                updated_at = NOW()
                            """,
                            (
                                rec.run_id,
                                step_name,
                                int(att.get("attempt_no", step.get("attempt_no", 1))),
                                step_script,
                                bool(att.get("success", step_success)),
                                bool(att.get("skipped", step_skipped)),
                                att.get("error", step_error),
                                att_outputs_json,
                                att.get("started_at", rec.started_at),
                                att.get("ended_at", rec.ended_at),
                            ),
                        )
                elif int(step.get("attempt_no", 0) or 0) > 0:
                    # Backward-compatible fallback when attempt history is not present.
                    cur.execute(
                        """
                        INSERT INTO etl_run_step_attempts (
                            run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at, updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, NOW())
                        ON CONFLICT (run_id, step_name, attempt_no)
                        DO UPDATE SET
                            script = EXCLUDED.script,
                            success = EXCLUDED.success,
                            skipped = EXCLUDED.skipped,
                            error = EXCLUDED.error,
                            outputs_json = EXCLUDED.outputs_json,
                            started_at = EXCLUDED.started_at,
                            ended_at = EXCLUDED.ended_at,
                            updated_at = NOW()
                        """,
                        (
                            rec.run_id,
                            step_name,
                            int(step.get("attempt_no", 1)),
                            step_script,
                            step_success,
                            step_skipped,
                            step_error,
                            outputs_json,
                            rec.started_at,
                            rec.ended_at,
                        ),
                    )

            event_details = json.dumps(
                {
                    "status": rec.status,
                    "success": rec.success,
                    "step_count": len(rec.steps),
                }
            )
            cur.execute(
                """
                INSERT INTO etl_run_events (run_id, event_type, details_json)
                VALUES (%s, %s, %s::jsonb)
                """,
                (rec.run_id, "run_completed", event_details),
            )
        conn.commit()


def upsert_run_status(
    *,
    run_id: str,
    pipeline: str,
    status: str,
    success: bool,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
    message: str = "",
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
    event_type: Optional[str] = None,
    event_details: Optional[Dict[str, Any]] = None,
) -> None:
    db_url = get_database_url()
    if not db_url:
        return

    started = started_at or _now_iso()
    ended = ended_at or _now_iso()
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_runs (
                    run_id, pipeline, success, status, started_at, ended_at, message, executor, artifact_dir
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id)
                DO UPDATE SET
                    pipeline = EXCLUDED.pipeline,
                    success = EXCLUDED.success,
                    status = EXCLUDED.status,
                    started_at = LEAST(etl_runs.started_at, EXCLUDED.started_at),
                    ended_at = GREATEST(etl_runs.ended_at, EXCLUDED.ended_at),
                    message = EXCLUDED.message,
                    executor = EXCLUDED.executor,
                    artifact_dir = EXCLUDED.artifact_dir
                """,
                (run_id, pipeline, success, status, started, ended, message, executor, artifact_dir),
            )
            if event_type:
                details_json = json.dumps(event_details or {})
                cur.execute(
                    """
                    INSERT INTO etl_run_events (run_id, event_type, details_json)
                    VALUES (%s, %s, %s::jsonb)
                    """,
                    (run_id, event_type, details_json),
                )
        conn.commit()


def upsert_step_attempt(
    *,
    run_id: str,
    step_name: str,
    attempt_no: int,
    script: str,
    success: bool,
    skipped: bool = False,
    error: Optional[str] = None,
    outputs: Optional[Dict[str, Any]] = None,
    started_at: Optional[str] = None,
    ended_at: Optional[str] = None,
) -> None:
    """
    Upsert a single step attempt row for retry-aware tracking.
    """
    db_url = get_database_url()
    if not db_url:
        return
    started = started_at or _now_iso()
    ended = ended_at or _now_iso()
    outputs_json = json.dumps(outputs or {}, default=str)
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_run_steps (
                    run_id, step_name, script, success, skipped, error, outputs_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (run_id, step_name)
                DO UPDATE SET
                    script = EXCLUDED.script,
                    success = EXCLUDED.success,
                    skipped = EXCLUDED.skipped,
                    error = EXCLUDED.error,
                    outputs_json = EXCLUDED.outputs_json
                """,
                (
                    run_id,
                    step_name,
                    script,
                    success,
                    skipped,
                    error,
                    outputs_json,
                ),
            )
            cur.execute(
                """
                INSERT INTO etl_run_step_attempts (
                    run_id, step_name, attempt_no, script, success, skipped, error, outputs_json, started_at, ended_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, NOW())
                ON CONFLICT (run_id, step_name, attempt_no)
                DO UPDATE SET
                    script = EXCLUDED.script,
                    success = EXCLUDED.success,
                    skipped = EXCLUDED.skipped,
                    error = EXCLUDED.error,
                    outputs_json = EXCLUDED.outputs_json,
                    started_at = EXCLUDED.started_at,
                    ended_at = EXCLUDED.ended_at,
                    updated_at = NOW()
                """,
                (
                    run_id,
                    step_name,
                    attempt_no,
                    script,
                    success,
                    skipped,
                    error,
                    outputs_json,
                    started,
                    ended,
                ),
            )
        conn.commit()


def record_run(
    run_result: RunResult,
    pipeline_path: str,
    store: Path,
    *,
    executor: Optional[str] = None,
    artifact_dir: Optional[str] = None,
) -> RunRecord:
    store.parent.mkdir(parents=True, exist_ok=True)
    started = getattr(run_result, "started_at", None) or _now_iso()
    ended = getattr(run_result, "ended_at", None) or _now_iso()
    rec = RunRecord(
        run_id=run_result.run_id,
        pipeline=str(pipeline_path),
        success=run_result.success,
        status="succeeded" if run_result.success else "failed",
        started_at=started,
        ended_at=ended,
        steps=[_step_to_dict(s) for s in run_result.steps],
    )
    with store.open("a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(rec)) + "\n")

    _upsert_run_db(rec, executor=executor, artifact_dir=artifact_dir)
    return rec


def load_runs(store: Path) -> List[RunRecord]:
    if not store.exists():
        return []
    records: List[RunRecord] = []
    with store.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            records.append(RunRecord(**data))
    return records


def find_run(store: Path, run_id: str) -> Optional[RunRecord]:
    for rec in load_runs(store):
        if rec.run_id == run_id:
            return rec
    return None


def load_run_step_states(run_id: str) -> Dict[str, RunStepState]:
    """
    Load latest step states for a run from DB tracking tables.
    """
    db_url = get_database_url()
    if not db_url:
        raise RuntimeError(
            "ETL_DATABASE_URL is required for --resume-run-id because step state is loaded from DB."
        )

    states: Dict[str, RunStepState] = {}
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT step_name, success, skipped, COALESCE(outputs_json, '{}'::jsonb)
                FROM etl_run_steps
                WHERE run_id = %s
                """,
                (run_id,),
            )
            for step_name, success, skipped, outputs in cur.fetchall():
                if isinstance(outputs, str):
                    outputs = json.loads(outputs or "{}")
                states[step_name] = RunStepState(
                    step_name=step_name,
                    success=bool(success),
                    skipped=bool(skipped),
                    outputs=outputs or {},
                )
    return states


__all__ = [
    "RunRecord",
    "RunStepState",
    "record_run",
    "load_runs",
    "find_run",
    "load_run_step_states",
    "upsert_run_status",
    "upsert_step_attempt",
]
